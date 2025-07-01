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

//! Basic example of using the Disney Kinesis Client library.
//!
//! This example shows how to:
//! - Set up AWS clients
//! - Create a simple record processor
//! - Start consuming from a Kinesis stream
//! - Handle graceful shutdown
//!
//! Usage:
//!   cargo run --example basic_consumer -- --stream-name YOUR_STREAM_NAME --table-name YOUR_TABLE_NAME
//!
//! Environment variables:
//!   AWS_REGION: AWS region (defaults to us-east-1)
//!   AWS_PROFILE: AWS profile to use (optional)

use std::sync::Arc;
use async_trait::async_trait;
use clap::Parser;
use disney_kinesis_client::*;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "basic_consumer")]
#[command(about = "A basic Kinesis consumer example")]
struct Args {
    #[arg(short, long)]
    stream_name: String,
    
    #[arg(short, long)]
    table_name: String,
    
    #[arg(short, long, default_value = "10")]
    max_leases: usize,
    
    #[arg(short, long, default_value = "1000")]
    limit: i32,
}

/// A simple record processor that logs each record
struct SimpleProcessor {
    checkpointer: Checkpointer,
    records_processed: std::sync::atomic::AtomicU64,
}

/// Factory to create our simple processor
struct SimpleProcessorFactory;

#[async_trait]
impl ShardRecordProcessorFactory for SimpleProcessorFactory {
    async fn initialize(&self, checkpointer: Checkpointer) -> Box<dyn ShardRecordProcessor> {
        info!("Initializing processor for shard: {}", checkpointer.lease_key);
        Box::new(SimpleProcessor {
            checkpointer,
            records_processed: std::sync::atomic::AtomicU64::new(0),
        })
    }
}

#[async_trait]
impl ShardRecordProcessor for SimpleProcessor {
    async fn process_records(&self, records: Vec<KinesisClientRecord>, millis_behind_latest: i64) {
        let count = records.len();
        
        info!(
            "Processing {} records for shard: {} ({}ms behind latest)",
            count, self.checkpointer.lease_key, millis_behind_latest
        );

        // Process each record
        for record in &records {
            let data = String::from_utf8_lossy(record.record.data.as_ref());
            info!(
                "Record - Partition Key: {}, Sequence: {}, Data: {}",
                record.record.partition_key.as_str(),
                record.record.sequence_number.as_str(),
                data.chars().take(100).collect::<String>() // Truncate for logging
            );
        }

        // Update our counter
        self.records_processed.fetch_add(
            count as u64, 
            std::sync::atomic::Ordering::Relaxed
        );

        // Checkpoint the last record - this is important for resuming processing
        // after restarts and for lease coordination
        if let Some(last_record) = records.last() {
            let checkpoint = last_record.extended_sequence_number();
            match self.checkpointer.checkpoint(checkpoint).await {
                Ok(_) => info!("Checkpointed successfully for shard: {}", self.checkpointer.lease_key),
                Err(e) => warn!("Failed to checkpoint for shard {}: {:?}", self.checkpointer.lease_key, e),
            }
        }
    }

    async fn lease_lost(&self) {
        let processed = self.records_processed.load(std::sync::atomic::Ordering::Relaxed);
        info!(
            "Lease lost for shard: {} (processed {} records)", 
            self.checkpointer.lease_key, processed
        );
    }

    async fn shard_ended(&self) {
        let processed = self.records_processed.load(std::sync::atomic::Ordering::Relaxed);
        info!(
            "Shard ended: {} (processed {} records)", 
            self.checkpointer.lease_key, processed
        );
        
        // Mark the shard as ended - this is important for proper cleanup
        if let Err(e) = self.checkpointer.end().await {
            warn!("Failed to end shard {}: {:?}", self.checkpointer.lease_key, e);
        }
    }

    async fn shutdown_requested(&self) {
        let processed = self.records_processed.load(std::sync::atomic::Ordering::Relaxed);
        info!(
            "Shutdown requested for shard: {} (processed {} records)", 
            self.checkpointer.lease_key, processed
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let args = Args::parse();

    info!("Starting Kinesis consumer for stream: {}", args.stream_name);

    // Initialize AWS clients
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let cloudwatch_client = Arc::new(aws_sdk_cloudwatch::Client::new(&config));
    let dynamodb_client = Arc::new(aws_sdk_dynamodb::Client::new(&config));
    let kinesis_client = Arc::new(aws_sdk_kinesis::Client::new(&config));

    // Create management configuration
    let management_config = ManagementConfig {
        leases_to_acquire: 2, // Conservative - grab 2 leases at a time
        max_leases: args.max_leases,
        start_strategy: StartStrategy::Latest, // Start from latest records
    };

    // Create lease configuration for polling mode
    let lease_config = LeaseConfig::PollingConfig { 
        limit: args.limit // Max records per GetRecords call
    };

    // Create the processor factory
    let processor_factory = Arc::new(SimpleProcessorFactory);

    // Create the client
    let client = Client::new(
        processor_factory,
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

    info!("Consumer started successfully. Press Ctrl+C to stop.");

    // Set up signal handling for graceful shutdown
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    // Wait for shutdown signal
    tokio::select! {
        _ = &mut ctrl_c => {
            info!("Received shutdown signal, stopping consumer...");
        }
    }

    // Graceful shutdown
    comms.shutdown().await;
    info!("Consumer stopped gracefully");

    Ok(())
}
