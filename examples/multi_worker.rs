// Copyright 2024-2025 Sam Halliday, Rob Pickerill (The Walt Disney Company)
//
// Licensed under the Tomorrow Open Source Technology License, Version 1.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  https://disneystreaming.github.io/TOST-1.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Multi-worker example demonstrating distributed consumption.
//!
//! This example shows how to run multiple consumer workers in parallel,
//! demonstrating the distributed lease management capabilities of the library.
//! Each worker will compete for leases and process different shards.
//!
//! Usage:
//!   cargo run --example multi_worker -- --stream-name YOUR_STREAM_NAME --table-name YOUR_TABLE_NAME --workers 3

use std::{sync::Arc, time::Duration, collections::HashMap};
use async_trait::async_trait;
use clap::Parser;
use disney_kinesis_client::*;
use tokio::{sync::RwLock, time::sleep};
use tracing::{info, warn, error};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "multi_worker")]
#[command(about = "Multi-worker Kinesis consumer example")]
struct Args {
    #[arg(short, long)]
    stream_name: String,
    
    #[arg(short, long)]
    table_name: String,
    
    #[arg(short, long, default_value = "3")]
    workers: usize,
    
    #[arg(long, default_value = "60")]
    run_duration_seconds: u64,
}

/// Shared statistics across all workers
#[derive(Debug, Default, Clone)]
struct GlobalStats {
    total_records_processed: u64,
    total_bytes_processed: u64,
    active_shards: HashMap<String, String>, // shard_id -> worker_id
    worker_stats: HashMap<String, WorkerStats>,
}

#[derive(Debug, Default, Clone)]
struct WorkerStats {
    records_processed: u64,
    bytes_processed: u64,
    active_shards: Vec<String>,
    start_time: Option<std::time::Instant>,
}

/// A processor that tracks statistics and coordinates with other workers
struct MultiWorkerProcessor {
    checkpointer: Checkpointer,
    worker_id: String,
    global_stats: Arc<RwLock<GlobalStats>>,
    local_stats: Arc<RwLock<WorkerStats>>,
}

/// Factory that creates processors and manages global coordination
struct MultiWorkerProcessorFactory {
    worker_id: String,
    global_stats: Arc<RwLock<GlobalStats>>,
}

impl MultiWorkerProcessorFactory {
    fn new(worker_id: String) -> Self {
        Self {
            worker_id: worker_id.clone(),
            global_stats: Arc::new(RwLock::new(GlobalStats::default())),
        }
    }
    
    fn clone_with_shared_stats(&self, worker_id: String) -> Self {
        Self {
            worker_id,
            global_stats: self.global_stats.clone(),
        }
    }

    async fn print_global_stats(&self) {
        let stats = self.global_stats.read().await;
        
        info!("=== Global Statistics ===");
        info!("Total records processed: {}", stats.total_records_processed);
        info!("Total bytes processed: {}", stats.total_bytes_processed);
        info!("Active shards: {}", stats.active_shards.len());
        
        for (shard_id, worker_id) in &stats.active_shards {
            info!("  Shard {} -> Worker {}", shard_id, worker_id);
        }
        
        info!("Worker Statistics:");
        for (worker_id, worker_stats) in &stats.worker_stats {
            let duration = worker_stats.start_time
                .map(|t| t.elapsed().as_secs())
                .unwrap_or(0);
            
            info!(
                "  Worker {}: {} records, {} bytes, {} shards, running for {}s",
                worker_id,
                worker_stats.records_processed,
                worker_stats.bytes_processed,
                worker_stats.active_shards.len(),
                duration
            );
        }
        info!("========================");
    }
}

#[async_trait]
impl ShardRecordProcessorFactory for MultiWorkerProcessorFactory {
    async fn initialize(&self, checkpointer: Checkpointer) -> Box<dyn ShardRecordProcessor> {
        let shard_id = checkpointer.lease_key.clone();
        let worker_id = self.worker_id.clone();
        
        info!("Worker {} acquired shard: {}", worker_id, shard_id);
        
        // Update global statistics
        {
            let mut stats = self.global_stats.write().await;
            stats.active_shards.insert(shard_id.clone(), worker_id.clone());
            
            let worker_stats = stats.worker_stats.entry(worker_id.clone()).or_default();
            worker_stats.active_shards.push(shard_id.clone());
            if worker_stats.start_time.is_none() {
                worker_stats.start_time = Some(std::time::Instant::now());
            }
        }
        
        let local_stats = Arc::new(RwLock::new(WorkerStats {
            start_time: Some(std::time::Instant::now()),
            ..Default::default()
        }));

        Box::new(MultiWorkerProcessor {
            checkpointer,
            worker_id,
            global_stats: self.global_stats.clone(),
            local_stats,
        })
    }
}

#[async_trait]
impl ShardRecordProcessor for MultiWorkerProcessor {
    async fn process_records(&self, records: Vec<KinesisClientRecord>, millis_behind_latest: i64) {
        let count = records.len();
        let shard_id = &self.checkpointer.lease_key;
        
        info!(
            "Worker {} processing {} records for shard {} ({}ms behind)",
            self.worker_id, count, shard_id, millis_behind_latest
        );

        let mut total_bytes = 0;
        
        // Simulate processing with some variety in processing time
        for (i, record) in records.iter().enumerate() {
            total_bytes += record.record.data.as_ref().len();
            
            // Simulate different processing patterns
            if i % 10 == 0 {
                // Occasionally simulate slower processing
                sleep(Duration::from_millis(10)).await;
            }
            
            // Log some records for visibility
            if i < 3 || i == records.len() - 1 {
                let data_preview = String::from_utf8_lossy(record.record.data.as_ref());
                info!(
                    "Worker {} [{}]: Record {} - {}",
                    self.worker_id,
                    shard_id,
                    i,
                    data_preview.chars().take(50).collect::<String>()
                );
            }
        }

        // Update local statistics
        {
            let mut local = self.local_stats.write().await;
            local.records_processed += count as u64;
            local.bytes_processed += total_bytes as u64;
        }

        // Update global statistics
        {
            let mut global = self.global_stats.write().await;
            global.total_records_processed += count as u64;
            global.total_bytes_processed += total_bytes as u64;
            
            if let Some(worker_stats) = global.worker_stats.get_mut(&self.worker_id) {
                worker_stats.records_processed += count as u64;
                worker_stats.bytes_processed += total_bytes as u64;
            }
        }

        // Checkpoint regularly
        if let Some(last_record) = records.last() {
            let checkpoint = last_record.extended_sequence_number();
            match self.checkpointer.checkpoint(checkpoint).await {
                Ok(_) => {
                    info!("Worker {} checkpointed shard {}", self.worker_id, shard_id);
                }
                Err(e) => {
                    warn!("Worker {} failed to checkpoint shard {}: {:?}", 
                          self.worker_id, shard_id, e);
                }
            }
        }
    }

    async fn lease_lost(&self) {
        let shard_id = &self.checkpointer.lease_key;
        
        warn!("Worker {} lost lease for shard {}", self.worker_id, shard_id);
        
        // Update global statistics
        {
            let mut stats = self.global_stats.write().await;
            stats.active_shards.remove(shard_id);
            
            if let Some(worker_stats) = stats.worker_stats.get_mut(&self.worker_id) {
                worker_stats.active_shards.retain(|s| s != shard_id);
            }
        }
    }

    async fn shard_ended(&self) {
        let shard_id = &self.checkpointer.lease_key;
        
        info!("Worker {} finished shard {}", self.worker_id, shard_id);
        
        // Update global statistics
        {
            let mut stats = self.global_stats.write().await;
            stats.active_shards.remove(shard_id);
            
            if let Some(worker_stats) = stats.worker_stats.get_mut(&self.worker_id) {
                worker_stats.active_shards.retain(|s| s != shard_id);
            }
        }
        
        if let Err(e) = self.checkpointer.end().await {
            warn!("Worker {} failed to end shard {}: {:?}", 
                  self.worker_id, shard_id, e);
        }
    }

    async fn shutdown_requested(&self) {
        let (local_records, local_bytes) = {
            let stats = self.local_stats.read().await;
            (stats.records_processed, stats.bytes_processed)
        };
        
        info!(
            "Worker {} shutting down (processed {} records, {} bytes)",
            self.worker_id, local_records, local_bytes
        );
    }
}

async fn create_worker(
    worker_id: String,
    factory: Arc<MultiWorkerProcessorFactory>,
    stream_name: String,
    table_name: String,
) -> Result<Comms, Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting worker: {}", worker_id);

    // Initialize AWS clients for this worker
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let cloudwatch_client = Arc::new(aws_sdk_cloudwatch::Client::new(&config));
    let dynamodb_client = Arc::new(aws_sdk_dynamodb::Client::new(&config));
    let kinesis_client = Arc::new(aws_sdk_kinesis::Client::new(&config));

    // Create management configuration - each worker will compete for leases
    let management_config = ManagementConfig {
        leases_to_acquire: 2, // Conservative - don't grab too many at once
        max_leases: 10,       // Reasonable limit per worker
        start_strategy: StartStrategy::Latest,
    };

    // Create lease configuration
    let lease_config = LeaseConfig::PollingConfig { 
        limit: 500 // Smaller batches for faster lease rotation
    };

    // Create the client
    let client = Client::new(
        factory,
        cloudwatch_client,
        dynamodb_client,
        kinesis_client,
        table_name,
        stream_name,
        management_config,
        lease_config,
    ).await;

    // Start the consumer
    let comms = client.run().await?;
    Ok(comms)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info,disney_kinesis_client=debug")
        .init();

    let args = Args::parse();

    info!("Starting {} workers for stream: {}", args.workers, args.stream_name);

    // Create the master factory (first worker will own the global stats)
    let master_worker_id = format!("worker-{}", Uuid::new_v4().to_string()[..8].to_string());
    let master_factory = Arc::new(MultiWorkerProcessorFactory::new(master_worker_id.clone()));

    // Start all workers
    let mut worker_handles = Vec::new();
    let mut worker_comms = Vec::new();

    for i in 0..args.workers {
        let worker_id = if i == 0 {
            master_worker_id.clone()
        } else {
            format!("worker-{}", Uuid::new_v4().to_string()[..8].to_string())
        };
        
        let factory = if i == 0 {
            master_factory.clone()
        } else {
            Arc::new(master_factory.clone_with_shared_stats(worker_id.clone()))
        };

        let stream_name = args.stream_name.clone();
        let table_name = args.table_name.clone();
        
        let handle = tokio::spawn(async move {
            create_worker(worker_id, factory, stream_name, table_name).await
        });
        
        worker_handles.push(handle);
    }

    // Wait for all workers to start
    for handle in worker_handles {
        match handle.await? {
            Ok(comms) => worker_comms.push(comms),
            Err(e) => error!("Failed to start worker: {:?}", e),
        }
    }

    info!("All {} workers started successfully", worker_comms.len());

    // Spawn a task to periodically print statistics
    let stats_factory = master_factory.clone();
    let stats_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        loop {
            interval.tick().await;
            stats_factory.print_global_stats().await;
        }
    });

    // Run for the specified duration or until Ctrl+C
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    tokio::select! {
        _ = &mut ctrl_c => {
            info!("Received shutdown signal");
        }
        _ = sleep(Duration::from_secs(args.run_duration_seconds)) => {
            info!("Reached maximum run duration");
        }
    }

    // Stop the statistics task
    stats_task.abort();

    // Print final statistics
    master_factory.print_global_stats().await;

    // Gracefully shutdown all workers
    info!("Shutting down all workers...");
    for (i, comms) in worker_comms.into_iter().enumerate() {
        info!("Shutting down worker {}", i);
        comms.shutdown().await;
    }

    info!("All workers stopped gracefully");
    Ok(())
}
