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

//! Simple Kinesis producer example for testing with the consumer examples.
//!
//! This example produces test data to a Kinesis stream that can be consumed
//! by the consumer examples in this directory.
//!
//! Usage:
//!   cargo run --example simple_producer -- --stream-name YOUR_STREAM_NAME --count 100

use std::time::{SystemTime, UNIX_EPOCH};
use aws_sdk_kinesis::primitives::Blob;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};
use tracing::{info, warn, error};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "simple_producer")]
#[command(about = "A simple Kinesis producer for testing")]
struct Args {
    #[arg(short, long)]
    stream_name: String,
    
    #[arg(short, long, default_value = "100")]
    count: usize,
    
    #[arg(short, long, default_value = "1000")]
    interval_ms: u64,
    
    #[arg(long, default_value = "false")]
    json_format: bool,
}

/// Example message structure that matches the consumer examples
#[derive(Debug, Serialize, Deserialize)]
struct TestMessage {
    id: String,
    timestamp: i64,
    event_type: String,
    payload: serde_json::Value,
}

impl TestMessage {
    fn new_user_action(user_id: &str) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            event_type: "user_action".to_string(),
            payload: serde_json::json!({
                "user_id": user_id,
                "action": "click",
                "page": "/home",
                "metadata": {
                    "browser": "Chrome",
                    "version": "96.0"
                }
            }),
        }
    }
    
    fn new_system_event(service: &str) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            event_type: "system_event".to_string(),
            payload: serde_json::json!({
                "service": service,
                "status": "healthy",
                "cpu_usage": 45.2,
                "memory_usage": 67.8
            }),
        }
    }
    
    fn new_error_event(error_code: &str) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            event_type: "error_event".to_string(),
            payload: serde_json::json!({
                "error_code": error_code,
                "message": "Something went wrong",
                "stack_trace": "at main.rs:123",
                "severity": "high"
            }),
        }
    }
}

async fn put_record(
    client: &aws_sdk_kinesis::Client,
    stream_name: &str,
    partition_key: &str,
    data: Vec<u8>,
) -> Result<(), aws_sdk_kinesis::Error> {
    let result = client
        .put_record()
        .stream_name(stream_name)
        .partition_key(partition_key)
        .data(Blob::new(data))
        .send()
        .await?;
    
    info!(
        "Record sent successfully - Shard ID: {}, Sequence Number: {}",
        result.shard_id(),
        result.sequence_number()
    );
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let args = Args::parse();

    info!("Starting Kinesis producer for stream: {}", args.stream_name);
    info!("Will produce {} records with {}ms interval", args.count, args.interval_ms);

    // Initialize AWS Kinesis client
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let kinesis_client = aws_sdk_kinesis::Client::new(&config);

    // Verify stream exists
    match kinesis_client
        .describe_stream()
        .stream_name(&args.stream_name)
        .send()
        .await
    {
        Ok(response) => {
            let stream = response.stream_description().unwrap();
            info!(
                "Stream found: {} (Status: {})",
                stream.stream_name(),
                stream.stream_status().as_str()
            );
            
            if stream.stream_status().as_str() != "ACTIVE" {
                warn!("Stream is not ACTIVE, continuing anyway...");
            }
        }
        Err(e) => {
            error!("Failed to describe stream: {:?}", e);
            return Err(e.into());
        }
    }

    let mut successful_sends = 0;
    let mut failed_sends = 0;

    for i in 0..args.count {
        let (partition_key, data) = if args.json_format {
            // Generate different types of test messages
            let message = match i % 10 {
                0..=6 => TestMessage::new_user_action(&format!("user_{}", i % 1000)),
                7..=8 => TestMessage::new_system_event(&format!("service_{}", i % 10)),
                9 => TestMessage::new_error_event(&format!("ERR_{}", i % 100)),
                _ => unreachable!(),
            };
            
            let json_data = serde_json::to_vec(&message)?;
            (message.id.clone(), json_data)
        } else {
            // Generate simple text records
            let partition_key = format!("partition_{}", i % 10);
            let message = format!(
                "Record {} - Generated at {} - Random data: {}",
                i,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                Uuid::new_v4()
            );
            (partition_key, message.into_bytes())
        };

        match put_record(&kinesis_client, &args.stream_name, &partition_key, data).await {
            Ok(_) => {
                successful_sends += 1;
                if (i + 1) % 10 == 0 {
                    info!("Sent {} records successfully", i + 1);
                }
            }
            Err(e) => {
                failed_sends += 1;
                error!("Failed to send record {}: {:?}", i, e);
            }
        }

        // Rate limiting
        if args.interval_ms > 0 {
            sleep(Duration::from_millis(args.interval_ms)).await;
        }
    }

    info!(
        "Finished sending records. Successful: {}, Failed: {}",
        successful_sends, failed_sends
    );

    Ok(())
}
