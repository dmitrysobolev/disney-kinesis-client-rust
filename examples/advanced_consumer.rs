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

//! Advanced example showing more sophisticated Kinesis record processing.
//!
//! This example demonstrates:
//! - Batched processing with periodic checkpointing
//! - Error handling and retry logic
//! - Metrics collection
//! - JSON message parsing
//! - Processing from a specific timestamp
//! - Graceful shutdown with work completion
//!
//! Usage:
//!   cargo run --example advanced_consumer -- --stream-name YOUR_STREAM_NAME --table-name YOUR_TABLE_NAME

use std::{sync::Arc, time::Duration, collections::HashMap};
use async_trait::async_trait;
use clap::Parser;
use disney_kinesis_client::*;
use serde::{Deserialize, Serialize};
use tokio::{sync::RwLock, time::Instant};
use tracing::{info, warn, error, debug};

#[derive(Parser)]
#[command(name = "advanced_consumer")]
#[command(about = "An advanced Kinesis consumer example with batching and metrics")]
struct Args {
    #[arg(short, long)]
    stream_name: String,
    
    #[arg(short, long)]
    table_name: String,
    
    #[arg(short, long, default_value = "16")]
    max_leases: usize,
    
    #[arg(short, long, default_value = "1000")]
    batch_size: usize,
    
    #[arg(long, default_value = "30")]
    checkpoint_interval_seconds: u64,
    
    #[arg(long)]
    start_from_timestamp: Option<i64>,
}

/// Example message structure - adapt this to your data format
#[derive(Debug, Deserialize, Serialize)]
struct Message {
    id: String,
    timestamp: i64,
    event_type: String,
    payload: serde_json::Value,
}

/// Metrics collection for monitoring
#[derive(Debug, Default)]
struct ProcessorMetrics {
    records_processed: u64,
    bytes_processed: u64,
    parse_errors: u64,
    processing_errors: u64,
    last_checkpoint_time: Option<Instant>,
    last_record_timestamp: Option<i64>,
}

/// Advanced processor with batching, error handling, and metrics
struct AdvancedProcessor {
    checkpointer: Checkpointer,
    metrics: Arc<RwLock<ProcessorMetrics>>,
    batch: Arc<RwLock<Vec<KinesisClientRecord>>>,
    checkpoint_interval: Duration,
    batch_size: usize,
    last_checkpoint: Arc<RwLock<Option<ExtendedSequenceNumber>>>,
}

/// Factory for creating advanced processors
struct AdvancedProcessorFactory {
    metrics: Arc<RwLock<HashMap<String, Arc<RwLock<ProcessorMetrics>>>>>,
    checkpoint_interval: Duration,
    batch_size: usize,
}

impl AdvancedProcessorFactory {
    fn new(checkpoint_interval: Duration, batch_size: usize) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            checkpoint_interval,
            batch_size,
        }
    }
    
    async fn get_metrics_summary(&self) -> HashMap<String, ProcessorMetrics> {
        let metrics_map = self.metrics.read().await;
        let mut summary = HashMap::new();
        
        for (shard_id, metrics) in metrics_map.iter() {
            let metrics_clone = {
                let m = metrics.read().await;
                ProcessorMetrics {
                    records_processed: m.records_processed,
                    bytes_processed: m.bytes_processed,
                    parse_errors: m.parse_errors,
                    processing_errors: m.processing_errors,
                    last_checkpoint_time: m.last_checkpoint_time,
                    last_record_timestamp: m.last_record_timestamp,
                }
            };
            summary.insert(shard_id.clone(), metrics_clone);
        }
        
        summary
    }
}

#[async_trait]
impl ShardRecordProcessorFactory for AdvancedProcessorFactory {
    async fn initialize(&self, checkpointer: Checkpointer) -> Box<dyn ShardRecordProcessor> {
        let shard_id = checkpointer.lease_key.clone();
        info!("Initializing advanced processor for shard: {}", shard_id);
        
        let metrics = Arc::new(RwLock::new(ProcessorMetrics::default()));
        self.metrics.write().await.insert(shard_id, metrics.clone());
        
        Box::new(AdvancedProcessor {
            checkpointer,
            metrics,
            batch: Arc::new(RwLock::new(Vec::new())),
            checkpoint_interval: self.checkpoint_interval,
            batch_size: self.batch_size,
            last_checkpoint: Arc::new(RwLock::new(None)),
        })
    }
}

impl AdvancedProcessor {
    async fn process_message(&self, record: &KinesisClientRecord) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let data = record.record.data.as_ref();
        
        // Try to parse as JSON
        match serde_json::from_slice::<Message>(data) {
            Ok(message) => {
                debug!("Parsed message: {:?}", message);
                
                // Simulate some business logic processing
                match message.event_type.as_str() {
                    "user_action" => self.process_user_action(&message).await?,
                    "system_event" => self.process_system_event(&message).await?,
                    "error_event" => self.process_error_event(&message).await?,
                    _ => {
                        warn!("Unknown event type: {}", message.event_type);
                    }
                }
                
                // Update metrics
                let mut metrics = self.metrics.write().await;
                metrics.last_record_timestamp = Some(message.timestamp);
                
                Ok(())
            }
            Err(e) => {
                // Handle non-JSON data (maybe it's plain text or other format)
                let data_str = String::from_utf8_lossy(data);
                warn!("Failed to parse record as JSON: {}. Data: {}", e, 
                      data_str.chars().take(200).collect::<String>());
                
                let mut metrics = self.metrics.write().await;
                metrics.parse_errors += 1;
                
                // You might want to send this to a dead letter queue or alternative processing
                self.process_raw_data(&data_str).await?;
                
                Ok(())
            }
        }
    }
    
    async fn process_user_action(&self, message: &Message) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Processing user action: {}", message.id);
        // Implement your user action processing logic here
        // For example: update user state, trigger notifications, etc.
        Ok(())
    }
    
    async fn process_system_event(&self, message: &Message) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Processing system event: {}", message.id);
        // Implement your system event processing logic here
        // For example: update system metrics, trigger alerts, etc.
        Ok(())
    }
    
    async fn process_error_event(&self, message: &Message) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        warn!("Processing error event: {}", message.id);
        // Implement your error event processing logic here
        // For example: log to monitoring system, trigger alerts, etc.
        Ok(())
    }
    
    async fn process_raw_data(&self, data: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Processing raw data: {}", data.chars().take(100).collect::<String>());
        // Implement processing for non-JSON data
        Ok(())
    }
    
    async fn should_checkpoint(&self) -> bool {
        let metrics = self.metrics.read().await;
        
        match metrics.last_checkpoint_time {
            None => true, // First time
            Some(last) => last.elapsed() >= self.checkpoint_interval,
        }
    }
    
    async fn checkpoint_batch(&self) -> Result<(), Fail> {
        let batch = self.batch.read().await;
        
        if let Some(last_record) = batch.last() {
            let checkpoint = last_record.extended_sequence_number();
            self.checkpointer.checkpoint(checkpoint.clone()).await?;
            
            // Update metrics
            {
                let mut metrics = self.metrics.write().await;
                metrics.last_checkpoint_time = Some(Instant::now());
            }
            
            // Update last checkpoint
            {
                let mut last_checkpoint = self.last_checkpoint.write().await;
                *last_checkpoint = Some(checkpoint);
            }
            
            info!("Checkpointed batch of {} records for shard: {}", 
                  batch.len(), self.checkpointer.lease_key);
        }
        
        Ok(())
    }
}

#[async_trait]
impl ShardRecordProcessor for AdvancedProcessor {
    async fn process_records(&self, records: Vec<KinesisClientRecord>, millis_behind_latest: i64) {
        let count = records.len();
        
        info!(
            "Processing {} records for shard: {} ({}ms behind latest)",
            count, self.checkpointer.lease_key, millis_behind_latest
        );

        // Add to batch
        {
            let mut batch = self.batch.write().await;
            batch.extend(records);
        }

        // Process each record in the current batch
        let records_to_process = {
            let batch = self.batch.read().await;
            batch.clone()
        };
        
        let mut successful_count = 0;
        let mut total_bytes = 0;
        
        for record in &records_to_process {
            total_bytes += record.record.data.as_ref().len();
            
            match self.process_message(record).await {
                Ok(_) => successful_count += 1,
                Err(e) => {
                    error!("Failed to process record: {:?}", e);
                    let mut metrics = self.metrics.write().await;
                    metrics.processing_errors += 1;
                }
            }
        }

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.records_processed += successful_count;
            metrics.bytes_processed += total_bytes as u64;
        }

        // Check if we should checkpoint (either by batch size or time)
        let should_checkpoint_by_size = {
            let batch = self.batch.read().await;
            batch.len() >= self.batch_size
        };
        
        let should_checkpoint_by_time = self.should_checkpoint().await;
        
        if should_checkpoint_by_size || should_checkpoint_by_time {
            match self.checkpoint_batch().await {
                Ok(_) => {
                    // Clear the batch after successful checkpoint
                    let mut batch = self.batch.write().await;
                    batch.clear();
                }
                Err(e) => {
                    warn!("Failed to checkpoint batch for shard {}: {:?}", 
                          self.checkpointer.lease_key, e);
                }
            }
        }
    }

    async fn lease_lost(&self) {
        let (processed, errors) = {
            let metrics = self.metrics.read().await;
            (metrics.records_processed, metrics.processing_errors)
        };
        
        warn!(
            "Lease lost for shard: {} (processed {} records, {} errors)", 
            self.checkpointer.lease_key, processed, errors
        );
        
        // Try to checkpoint any remaining work before losing the lease
        let _ = self.checkpoint_batch().await;
    }

    async fn shard_ended(&self) {
        let (processed, errors) = {
            let metrics = self.metrics.read().await;
            (metrics.records_processed, metrics.processing_errors)
        };
        
        info!(
            "Shard ended: {} (processed {} records, {} errors)", 
            self.checkpointer.lease_key, processed, errors
        );
        
        // Process any remaining records in the batch
        let _ = self.checkpoint_batch().await;
        
        // Mark the shard as ended
        if let Err(e) = self.checkpointer.end().await {
            warn!("Failed to end shard {}: {:?}", self.checkpointer.lease_key, e);
        }
    }

    async fn shutdown_requested(&self) {
        let (processed, errors) = {
            let metrics = self.metrics.read().await;
            (metrics.records_processed, metrics.processing_errors)
        };
        
        info!(
            "Shutdown requested for shard: {} (processed {} records, {} errors)", 
            self.checkpointer.lease_key, processed, errors
        );
        
        // Gracefully finish processing current batch
        let _ = self.checkpoint_batch().await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info,disney_kinesis_client=debug")
        .init();

    let args = Args::parse();

    info!("Starting advanced Kinesis consumer for stream: {}", args.stream_name);

    // Initialize AWS clients
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let cloudwatch_client = Arc::new(aws_sdk_cloudwatch::Client::new(&config));
    let dynamodb_client = Arc::new(aws_sdk_dynamodb::Client::new(&config));
    let kinesis_client = Arc::new(aws_sdk_kinesis::Client::new(&config));

    // Determine start strategy
    let start_strategy = if let Some(timestamp) = args.start_from_timestamp {
        info!("Starting from timestamp: {}", timestamp);
        StartStrategy::Timestamp(timestamp)
    } else {
        info!("Starting from latest records");
        StartStrategy::Latest
    };

    // Create management configuration
    let management_config = ManagementConfig {
        leases_to_acquire: 3, // Moderate growth
        max_leases: args.max_leases,
        start_strategy,
    };

    // Create lease configuration
    let lease_config = LeaseConfig::PollingConfig { 
        limit: 1000 // Max records per GetRecords call
    };

    // Create the processor factory
    let checkpoint_interval = Duration::from_secs(args.checkpoint_interval_seconds);
    let processor_factory = Arc::new(AdvancedProcessorFactory::new(
        checkpoint_interval,
        args.batch_size,
    ));

    // Create the client
    let client = Client::new(
        processor_factory.clone(),
        cloudwatch_client,
        dynamodb_client,
        kinesis_client,
        args.table_name,
        args.stream_name,
        management_config,
        lease_config,
    ).await;

    // Start the consumer
    let comms = client.run().await?;

    info!("Advanced consumer started successfully. Press Ctrl+C to stop.");

    // Spawn a task to periodically log metrics
    let metrics_factory = processor_factory.clone();
    let metrics_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            
            let metrics = metrics_factory.get_metrics_summary().await;
            if !metrics.is_empty() {
                info!("=== Metrics Summary ===");
                for (shard_id, m) in metrics {
                    info!(
                        "Shard {}: {} records, {} bytes, {} parse errors, {} processing errors",
                        shard_id, m.records_processed, m.bytes_processed, 
                        m.parse_errors, m.processing_errors
                    );
                }
                info!("=====================");
            }
        }
    });

    // Set up signal handling for graceful shutdown
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    // Wait for shutdown signal
    tokio::select! {
        _ = &mut ctrl_c => {
            info!("Received shutdown signal, stopping consumer...");
        }
    }

    // Stop the metrics task
    metrics_task.abort();

    // Graceful shutdown
    comms.shutdown().await;
    info!("Advanced consumer stopped gracefully");

    Ok(())
}
