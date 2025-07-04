use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_kinesis::types::Record;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};

use disney_kinesis_client::{
    checkpointer::Checkpointer,
    client::KinesisClient,
    config::{KinesisClientConfig, LeaseConfig, ManagementConfig},
    factory::KinesisClientFactory,
    processor::{KinesisClientRecord, ShardRecordProcessor},
    types::StartStrategy,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Kinesis stream name
    #[clap(long)]
    stream_name: String,

    /// DynamoDB table name for lease management
    #[clap(long)]
    table_name: String,

    /// Application name (used for metrics and grouping)
    #[clap(long, default_value = "resilient-consumer")]
    app_name: String,

    /// Maximum retry attempts for failed records
    #[clap(long, default_value = "3")]
    max_retries: u32,

    /// Retry delay in milliseconds
    #[clap(long, default_value = "1000")]
    retry_delay_ms: u64,

    /// Circuit breaker error threshold
    #[clap(long, default_value = "10")]
    circuit_breaker_threshold: u64,

    /// Circuit breaker reset timeout in seconds
    #[clap(long, default_value = "60")]
    circuit_breaker_reset_seconds: u64,

    /// Processing timeout per record in milliseconds
    #[clap(long, default_value = "5000")]
    processing_timeout_ms: u64,

    /// Batch size for processing
    #[clap(long, default_value = "100")]
    batch_size: usize,

    /// Checkpoint interval in seconds
    #[clap(long, default_value = "30")]
    checkpoint_interval_seconds: u64,
}

#[derive(Deserialize, Serialize, Clone)]
struct ProcessingMessage {
    id: String,
    event_type: String,
    payload: serde_json::Value,
    timestamp: u64,
}

#[derive(Debug, Clone)]
enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug)]
struct CircuitBreaker {
    state: CircuitBreakerState,
    failure_count: AtomicU64,
    last_failure_time: Option<Instant>,
    failure_threshold: u64,
    reset_timeout: Duration,
}

impl CircuitBreaker {
    fn new(failure_threshold: u64, reset_timeout: Duration) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            failure_count: AtomicU64::new(0),
            last_failure_time: None,
            failure_threshold,
            reset_timeout,
        }
    }

    fn can_execute(&self) -> bool {
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                if let Some(last_failure) = self.last_failure_time {
                    last_failure.elapsed() > self.reset_timeout
                } else {
                    false
                }
            }
            CircuitBreakerState::HalfOpen => true,
        }
    }

    fn on_success(&mut self) {
        self.failure_count.store(0, Ordering::Relaxed);
        self.state = CircuitBreakerState::Closed;
        self.last_failure_time = None;
    }

    fn on_failure(&mut self) {
        let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.last_failure_time = Some(Instant::now());

        if failures >= self.failure_threshold {
            self.state = match self.state {
                CircuitBreakerState::Closed => CircuitBreakerState::Open,
                CircuitBreakerState::HalfOpen => CircuitBreakerState::Open,
                CircuitBreakerState::Open => CircuitBreakerState::Open,
            };
        }
    }
}

#[derive(Debug)]
struct ProcessingStats {
    total_records: AtomicU64,
    successful_records: AtomicU64,
    failed_records: AtomicU64,
    retried_records: AtomicU64,
    circuit_breaker_trips: AtomicU64,
    timeouts: AtomicU64,
    last_checkpoint: Arc<tokio::sync::RwLock<Option<Instant>>>,
}

impl ProcessingStats {
    fn new() -> Self {
        Self {
            total_records: AtomicU64::new(0),
            successful_records: AtomicU64::new(0),
            failed_records: AtomicU64::new(0),
            retried_records: AtomicU64::new(0),
            circuit_breaker_trips: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            last_checkpoint: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    async fn report(&self) {
        let total = self.total_records.load(Ordering::Relaxed);
        let successful = self.successful_records.load(Ordering::Relaxed);
        let failed = self.failed_records.load(Ordering::Relaxed);
        let retried = self.retried_records.load(Ordering::Relaxed);
        let circuit_trips = self.circuit_breaker_trips.load(Ordering::Relaxed);
        let timeouts = self.timeouts.load(Ordering::Relaxed);

        let success_rate = if total > 0 {
            (successful as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        info!(
            "Processing Stats - Total: {}, Success: {} ({:.2}%), Failed: {}, Retried: {}, Circuit Trips: {}, Timeouts: {}",
            total, successful, success_rate, failed, retried, circuit_trips, timeouts
        );
    }
}

struct ResilientProcessor {
    max_retries: u32,
    retry_delay: Duration,
    processing_timeout: Duration,
    circuit_breaker: Arc<tokio::sync::RwLock<CircuitBreaker>>,
    stats: Arc<ProcessingStats>,
    dead_letter_queue: Arc<tokio::sync::RwLock<Vec<ProcessingMessage>>>,
}

impl ResilientProcessor {
    fn new(
        max_retries: u32,
        retry_delay_ms: u64,
        processing_timeout_ms: u64,
        circuit_breaker_threshold: u64,
        circuit_breaker_reset_seconds: u64,
    ) -> Self {
        Self {
            max_retries,
            retry_delay: Duration::from_millis(retry_delay_ms),
            processing_timeout: Duration::from_millis(processing_timeout_ms),
            circuit_breaker: Arc::new(tokio::sync::RwLock::new(CircuitBreaker::new(
                circuit_breaker_threshold,
                Duration::from_secs(circuit_breaker_reset_seconds),
            ))),
            stats: Arc::new(ProcessingStats::new()),
            dead_letter_queue: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }

    async fn process_message_with_retry(&self, message: ProcessingMessage) -> Result<(), String> {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts <= self.max_retries {
            // Check circuit breaker
            {
                let breaker = self.circuit_breaker.read().await;
                if !breaker.can_execute() {
                    self.stats.circuit_breaker_trips.fetch_add(1, Ordering::Relaxed);
                    return Err("Circuit breaker is open".to_string());
                }
            }

            // Process with timeout
            let process_result = timeout(
                self.processing_timeout,
                self.simulate_processing(&message)
            ).await;

            match process_result {
                Ok(Ok(_)) => {
                    // Success - update circuit breaker
                    {
                        let mut breaker = self.circuit_breaker.write().await;
                        breaker.on_success();
                    }
                    return Ok(());
                }
                Ok(Err(e)) => {
                    last_error = Some(e);
                    attempts += 1;
                    
                    if attempts <= self.max_retries {
                        self.stats.retried_records.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            "Processing failed for message {}, attempt {}/{}: {}. Retrying in {:?}",
                            message.id, attempts, self.max_retries + 1, last_error.as_ref().unwrap(), self.retry_delay
                        );
                        
                        // Exponential backoff
                        let delay = self.retry_delay * (2_u32.pow(attempts - 1));
                        sleep(delay).await;
                    }
                }
                Err(_) => {
                    // Timeout
                    self.stats.timeouts.fetch_add(1, Ordering::Relaxed);
                    last_error = Some("Processing timeout".to_string());
                    attempts += 1;
                    
                    if attempts <= self.max_retries {
                        warn!(
                            "Processing timeout for message {}, attempt {}/{}. Retrying in {:?}",
                            message.id, attempts, self.max_retries + 1, self.retry_delay
                        );
                        sleep(self.retry_delay).await;
                    }
                }
            }
        }

        // All retries exhausted - update circuit breaker and add to DLQ
        {
            let mut breaker = self.circuit_breaker.write().await;
            breaker.on_failure();
        }

        {
            let mut dlq = self.dead_letter_queue.write().await;
            dlq.push(message.clone());
        }

        Err(last_error.unwrap_or_else(|| "Unknown error".to_string()))
    }

    async fn simulate_processing(&self, message: &ProcessingMessage) -> Result<(), String> {
        // Simulate different types of processing based on event type
        match message.event_type.as_str() {
            "user_action" => {
                // Simulate user action processing
                info!("Processing user action: {}", message.id);
                
                // Simulate occasional failures
                if message.id.ends_with("err") {
                    return Err("Simulated user action processing error".to_string());
                }
                
                // Simulate processing time
                sleep(Duration::from_millis(100)).await;
                Ok(())
            }
            "system_event" => {
                // Simulate system event processing
                info!("Processing system event: {}", message.id);
                
                // Simulate network calls or database operations
                sleep(Duration::from_millis(200)).await;
                
                // Simulate occasional timeouts
                if message.id.ends_with("timeout") {
                    sleep(Duration::from_secs(10)).await; // This will timeout
                }
                
                Ok(())
            }
            "error_event" => {
                // Simulate error event processing
                warn!("Processing error event: {}", message.id);
                
                // Always fail these for demonstration
                Err("Error events always fail for demo".to_string())
            }
            _ => {
                // Unknown event type
                warn!("Unknown event type: {}", message.event_type);
                Ok(())
            }
        }
    }

    async fn report_dead_letter_queue(&self) {
        let dlq = self.dead_letter_queue.read().await;
        if !dlq.is_empty() {
            warn!("Dead Letter Queue contains {} messages:", dlq.len());
            for (i, msg) in dlq.iter().enumerate().take(5) {
                warn!("  DLQ[{}]: {} - {}", i, msg.id, msg.event_type);
            }
            if dlq.len() > 5 {
                warn!("  ... and {} more messages", dlq.len() - 5);
            }
        }
    }
}

#[async_trait]
impl ShardRecordProcessor for ResilientProcessor {
    async fn process_records(&self, records: Vec<KinesisClientRecord>, millis_behind_latest: i64) {
        let batch_size = records.len();
        info!(
            "Processing batch of {} records ({}ms behind latest)",
            batch_size, millis_behind_latest
        );

        let mut processed_count = 0;
        let mut failed_count = 0;
        let mut messages_to_process = Vec::new();

        // Parse all records first
        for record in &records {
            self.stats.total_records.fetch_add(1, Ordering::Relaxed);
            
            // Try to parse as JSON first
            match serde_json::from_slice::<ProcessingMessage>(record.record.data.as_ref()) {
                Ok(message) => {
                    messages_to_process.push(message);
                }
                Err(_) => {
                    // Fall back to creating a generic message
                    let data_str = String::from_utf8_lossy(record.record.data.as_ref());
                    let generic_message = ProcessingMessage {
                        id: format!("record-{}", record.record.sequence_number),
                        event_type: "unknown".to_string(),
                        payload: serde_json::Value::String(data_str.to_string()),
                        timestamp: chrono::Utc::now().timestamp_millis() as u64,
                    };
                    messages_to_process.push(generic_message);
                }
            }
        }

        // Process messages with resilience patterns
        for message in messages_to_process {
            match self.process_message_with_retry(message).await {
                Ok(_) => {
                    processed_count += 1;
                    self.stats.successful_records.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    failed_count += 1;
                    self.stats.failed_records.fetch_add(1, Ordering::Relaxed);
                    error!("Failed to process record after retries: {}", e);
                }
            }
        }

        info!(
            "Batch processing completed: {} successful, {} failed",
            processed_count, failed_count
        );

        // Update checkpoint timestamp
        {
            let mut last_checkpoint = self.stats.last_checkpoint.write().await;
            *last_checkpoint = Some(Instant::now());
        }
    }

    async fn lease_lost(&self) {
        warn!("Lease lost! Cleaning up resources...");
        
        // Report final stats
        self.stats.report().await;
        self.report_dead_letter_queue().await;
        
        info!("Lease lost cleanup completed");
    }

    async fn shard_ended(&self) {
        info!("Shard ended! Processing final statistics...");
        
        // Report final stats
        self.stats.report().await;
        self.report_dead_letter_queue().await;
        
        info!("Shard ended cleanup completed");
    }

    async fn shutdown_requested(&self) {
        info!("Shutdown requested! Performing graceful shutdown...");
        
        // Report final stats
        self.stats.report().await;
        self.report_dead_letter_queue().await;
        
        info!("Graceful shutdown completed");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info,disney_kinesis_client=debug")
        .init();

    let args = Args::parse();

    info!("Starting resilient consumer with configuration:");
    info!("  Stream: {}", args.stream_name);
    info!("  Table: {}", args.table_name);
    info!("  App: {}", args.app_name);
    info!("  Max Retries: {}", args.max_retries);
    info!("  Retry Delay: {}ms", args.retry_delay_ms);
    info!("  Circuit Breaker Threshold: {}", args.circuit_breaker_threshold);
    info!("  Processing Timeout: {}ms", args.processing_timeout_ms);

    // Create AWS config
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    // Create the resilient processor
    let processor = Arc::new(ResilientProcessor::new(
        args.max_retries,
        args.retry_delay_ms,
        args.processing_timeout_ms,
        args.circuit_breaker_threshold,
        args.circuit_breaker_reset_seconds,
    ));

    // Clone for stats reporting
    let stats_processor = processor.clone();

    // Start periodic stats reporting
    let stats_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            stats_processor.stats.report().await;
            stats_processor.report_dead_letter_queue().await;
        }
    });

    // Build Kinesis client configuration
    let kinesis_config = KinesisClientConfig::builder()
        .stream_name(args.stream_name)
        .application_name(args.app_name)
        .table_name(args.table_name)
        .aws_config(aws_config)
        .management_config(
            ManagementConfig::builder()
                .start_strategy(StartStrategy::Latest)
                .leases_to_acquire(1)
                .max_leases(10)
                .build(),
        )
        .lease_config(LeaseConfig::PollingConfig { 
            limit: args.batch_size 
        })
        .build();

    // Create and start the Kinesis client
    let factory = KinesisClientFactory::new(kinesis_config);
    let client = factory.create_client(processor).await?;

    info!("Starting Kinesis client...");
    
    // Handle graceful shutdown
    let shutdown_client = client.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Received Ctrl+C, initiating shutdown...");
        shutdown_client.shutdown().await;
    });

    // Start the client
    let result = client.start().await;
    
    // Cancel stats task
    stats_task.abort();
    
    match result {
        Ok(_) => info!("Kinesis client completed successfully"),
        Err(e) => error!("Kinesis client failed: {:?}", e),
    }

    Ok(())
}