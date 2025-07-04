use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, mpsc};
use tokio::time::{sleep, interval};
use tracing::{error, info, warn};
use uuid::Uuid;

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

    /// Number of worker instances to run
    #[clap(long, default_value = "3")]
    workers: usize,

    /// Duration to run in seconds (0 for unlimited)
    #[clap(long, default_value = "120")]
    run_duration_seconds: u64,

    /// Application name for metrics and grouping
    #[clap(long, default_value = "multi-worker-demo")]
    app_name: String,

    /// Batch size for record processing
    #[clap(long, default_value = "500")]
    batch_size: usize,

    /// Leases to acquire per worker
    #[clap(long, default_value = "2")]
    leases_to_acquire: usize,

    /// Maximum leases per worker
    #[clap(long, default_value = "10")]
    max_leases: usize,

    /// Statistics reporting interval in seconds
    #[clap(long, default_value = "15")]
    stats_interval_seconds: u64,

    /// Enable worker restart on failure
    #[clap(long)]
    auto_restart: bool,

    /// Worker failure restart delay in seconds
    #[clap(long, default_value = "30")]
    restart_delay_seconds: u64,

    /// Enable dynamic scaling (add/remove workers)
    #[clap(long)]
    dynamic_scaling: bool,

    /// Target records per second per worker for scaling
    #[clap(long, default_value = "100")]
    target_rps_per_worker: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerMessage {
    worker_id: String,
    message_type: String,
    payload: serde_json::Value,
    timestamp: u64,
}

#[derive(Debug, Clone)]
struct WorkerStats {
    worker_id: String,
    records_processed: AtomicU64,
    bytes_processed: AtomicU64,
    processing_errors: AtomicU64,
    active_shards: RwLock<Vec<String>>,
    start_time: Instant,
    last_activity: RwLock<Instant>,
    current_rps: RwLock<f64>,
    status: RwLock<WorkerStatus>,
}

#[derive(Debug, Clone, PartialEq)]
enum WorkerStatus {
    Starting,
    Running,
    Stopping,
    Failed,
    Restarting,
}

impl WorkerStats {
    fn new(worker_id: String) -> Self {
        let now = Instant::now();
        Self {
            worker_id,
            records_processed: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            processing_errors: AtomicU64::new(0),
            active_shards: RwLock::new(Vec::new()),
            start_time: now,
            last_activity: RwLock::new(now),
            current_rps: RwLock::new(0.0),
            status: RwLock::new(WorkerStatus::Starting),
        }
    }

    async fn update_activity(&self) {
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
    }

    async fn calculate_rps(&self) -> f64 {
        let records = self.records_processed.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            records as f64 / elapsed
        } else {
            0.0
        }
    }

    async fn update_rps(&self) {
        let rps = self.calculate_rps().await;
        let mut current_rps = self.current_rps.write().await;
        *current_rps = rps;
    }

    async fn is_healthy(&self) -> bool {
        let last_activity = self.last_activity.read().await;
        let status = self.status.read().await;
        
        match *status {
            WorkerStatus::Failed => false,
            WorkerStatus::Stopping => false,
            _ => last_activity.elapsed() < Duration::from_secs(120), // 2 minutes
        }
    }
}

#[derive(Debug)]
struct GlobalCoordinator {
    workers: RwLock<HashMap<String, Arc<WorkerStats>>>,
    total_records: AtomicU64,
    total_bytes: AtomicU64,
    total_errors: AtomicU64,
    active_shards: RwLock<HashMap<String, String>>, // shard_id -> worker_id
    start_time: Instant,
    target_rps_per_worker: f64,
}

impl GlobalCoordinator {
    fn new(target_rps_per_worker: f64) -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
            total_records: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            active_shards: RwLock::new(HashMap::new()),
            start_time: Instant::now(),
            target_rps_per_worker,
        }
    }

    async fn register_worker(&self, worker_stats: Arc<WorkerStats>) {
        let worker_id = worker_stats.worker_id.clone();
        let mut workers = self.workers.write().await;
        workers.insert(worker_id.clone(), worker_stats);
        info!("Registered worker: {}", worker_id);
    }

    async fn unregister_worker(&self, worker_id: &str) {
        let mut workers = self.workers.write().await;
        workers.remove(worker_id);
        info!("Unregistered worker: {}", worker_id);
    }

    async fn update_shard_assignment(&self, shard_id: String, worker_id: String) {
        let mut shards = self.active_shards.write().await;
        shards.insert(shard_id, worker_id);
    }

    async fn remove_shard_assignment(&self, shard_id: &str) {
        let mut shards = self.active_shards.write().await;
        shards.remove(shard_id);
    }

    async fn get_worker_count(&self) -> usize {
        let workers = self.workers.read().await;
        workers.len()
    }

    async fn get_healthy_worker_count(&self) -> usize {
        let workers = self.workers.read().await;
        let mut healthy_count = 0;
        for worker in workers.values() {
            if worker.is_healthy().await {
                healthy_count += 1;
            }
        }
        healthy_count
    }

    async fn should_scale_up(&self) -> bool {
        let workers = self.workers.read().await;
        let mut total_rps = 0.0;
        let mut active_workers = 0;

        for worker in workers.values() {
            if worker.is_healthy().await {
                total_rps += worker.calculate_rps().await;
                active_workers += 1;
            }
        }

        if active_workers == 0 {
            return false;
        }

        let avg_rps = total_rps / active_workers as f64;
        avg_rps > self.target_rps_per_worker * 1.5 // Scale up if 50% above target
    }

    async fn should_scale_down(&self) -> bool {
        let workers = self.workers.read().await;
        let mut total_rps = 0.0;
        let mut active_workers = 0;

        for worker in workers.values() {
            if worker.is_healthy().await {
                total_rps += worker.calculate_rps().await;
                active_workers += 1;
            }
        }

        if active_workers <= 1 {
            return false; // Never scale down below 1 worker
        }

        let avg_rps = total_rps / active_workers as f64;
        avg_rps < self.target_rps_per_worker * 0.5 // Scale down if 50% below target
    }

    async fn print_statistics(&self) {
        let workers = self.workers.read().await;
        let shards = self.active_shards.read().await;
        
        let total_records = self.total_records.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let total_errors = self.total_errors.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        
        let overall_rps = if elapsed > 0.0 {
            total_records as f64 / elapsed
        } else {
            0.0
        };

        info!("=== Multi-Worker Statistics ===");
        info!("Runtime: {:.1}s", elapsed);
        info!("Total Records: {}", total_records);
        info!("Total Bytes: {}", total_bytes);
        info!("Total Errors: {}", total_errors);
        info!("Overall RPS: {:.2}", overall_rps);
        info!("Active Workers: {}", workers.len());
        info!("Active Shards: {}", shards.len());

        let mut healthy_workers = 0;
        let mut total_worker_rps = 0.0;

        info!("Worker Details:");
        for (worker_id, worker_stats) in workers.iter() {
            let records = worker_stats.records_processed.load(Ordering::Relaxed);
            let bytes = worker_stats.bytes_processed.load(Ordering::Relaxed);
            let errors = worker_stats.processing_errors.load(Ordering::Relaxed);
            let worker_rps = worker_stats.calculate_rps().await;
            let worker_elapsed = worker_stats.start_time.elapsed().as_secs_f64();
            let status = worker_stats.status.read().await;
            let active_shards = worker_stats.active_shards.read().await;
            let is_healthy = worker_stats.is_healthy().await;

            if is_healthy {
                healthy_workers += 1;
                total_worker_rps += worker_rps;
            }

            info!(
                "  {}: {} records ({:.2} RPS), {} bytes, {} errors, {} shards, {:.1}s, {:?}, {}",
                worker_id,
                records,
                worker_rps,
                bytes,
                errors,
                active_shards.len(),
                worker_elapsed,
                *status,
                if is_healthy { "HEALTHY" } else { "UNHEALTHY" }
            );

            for shard_id in active_shards.iter() {
                info!("    Shard: {}", shard_id);
            }
        }

        if healthy_workers > 0 {
            let avg_rps = total_worker_rps / healthy_workers as f64;
            info!("Average RPS per healthy worker: {:.2}", avg_rps);
        }

        info!("Shard Distribution:");
        for (shard_id, worker_id) in shards.iter() {
            info!("  {} -> {}", shard_id, worker_id);
        }

        info!("===============================");
    }
}

struct MultiWorkerProcessor {
    worker_id: String,
    coordinator: Arc<GlobalCoordinator>,
    worker_stats: Arc<WorkerStats>,
}

impl MultiWorkerProcessor {
    fn new(worker_id: String, coordinator: Arc<GlobalCoordinator>) -> Self {
        let worker_stats = Arc::new(WorkerStats::new(worker_id.clone()));
        Self {
            worker_id,
            coordinator,
            worker_stats,
        }
    }

    async fn process_message(&self, message: &WorkerMessage) -> Result<(), String> {
        // Simulate different processing patterns based on message type
        match message.message_type.as_str() {
            "user_action" => {
                info!("Worker {} processing user action: {}", self.worker_id, message.worker_id);
                // Simulate user action processing
                sleep(Duration::from_millis(50)).await;
                Ok(())
            }
            "system_event" => {
                info!("Worker {} processing system event: {}", self.worker_id, message.worker_id);
                // Simulate system event processing
                sleep(Duration::from_millis(100)).await;
                Ok(())
            }
            "error_event" => {
                warn!("Worker {} processing error event: {}", self.worker_id, message.worker_id);
                // Simulate error processing (might fail)
                if message.worker_id.ends_with("err") {
                    return Err("Simulated processing error".to_string());
                }
                sleep(Duration::from_millis(200)).await;
                Ok(())
            }
            _ => {
                info!("Worker {} processing unknown message type: {}", self.worker_id, message.message_type);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl ShardRecordProcessor for MultiWorkerProcessor {
    async fn process_records(&self, records: Vec<KinesisClientRecord>, millis_behind_latest: i64) {
        let count = records.len();
        info!(
            "Worker {} processing {} records ({}ms behind latest)",
            self.worker_id, count, millis_behind_latest
        );

        let mut processed_count = 0;
        let mut failed_count = 0;
        let mut total_bytes = 0;

        for (i, record) in records.iter().enumerate() {
            total_bytes += record.record.data.len();

            // Try to parse as JSON message first
            match serde_json::from_slice::<WorkerMessage>(record.record.data.as_ref()) {
                Ok(message) => {
                    match self.process_message(&message).await {
                        Ok(_) => processed_count += 1,
                        Err(e) => {
                            failed_count += 1;
                            error!("Worker {} failed to process message: {}", self.worker_id, e);
                        }
                    }
                }
                Err(_) => {
                    // Fall back to processing as raw data
                    let data_preview = String::from_utf8_lossy(record.record.data.as_ref());
                    info!(
                        "Worker {} processing raw data [{}]: {}",
                        self.worker_id,
                        i,
                        data_preview.chars().take(50).collect::<String>()
                    );
                    processed_count += 1;
                }
            }
        }

        // Update worker statistics
        self.worker_stats.records_processed.fetch_add(processed_count, Ordering::Relaxed);
        self.worker_stats.bytes_processed.fetch_add(total_bytes as u64, Ordering::Relaxed);
        self.worker_stats.processing_errors.fetch_add(failed_count, Ordering::Relaxed);
        self.worker_stats.update_activity().await;
        self.worker_stats.update_rps().await;

        // Update global statistics
        self.coordinator.total_records.fetch_add(processed_count, Ordering::Relaxed);
        self.coordinator.total_bytes.fetch_add(total_bytes as u64, Ordering::Relaxed);
        self.coordinator.total_errors.fetch_add(failed_count, Ordering::Relaxed);

        info!(
            "Worker {} batch completed: {} processed, {} failed, {} bytes",
            self.worker_id, processed_count, failed_count, total_bytes
        );
    }

    async fn lease_lost(&self) {
        warn!("Worker {} lost lease", self.worker_id);
        
        // Update worker status
        let mut status = self.worker_stats.status.write().await;
        *status = WorkerStatus::Failed;
    }

    async fn shard_ended(&self) {
        info!("Worker {} shard ended", self.worker_id);
        
        // Update worker status
        let mut status = self.worker_stats.status.write().await;
        *status = WorkerStatus::Stopping;
    }

    async fn shutdown_requested(&self) {
        info!("Worker {} shutdown requested", self.worker_id);
        
        // Update worker status
        let mut status = self.worker_stats.status.write().await;
        *status = WorkerStatus::Stopping;
        
        // Final statistics
        let records = self.worker_stats.records_processed.load(Ordering::Relaxed);
        let bytes = self.worker_stats.bytes_processed.load(Ordering::Relaxed);
        let errors = self.worker_stats.processing_errors.load(Ordering::Relaxed);
        
        info!(
            "Worker {} final stats: {} records, {} bytes, {} errors",
            self.worker_id, records, bytes, errors
        );
    }
}

async fn create_worker(
    worker_id: String,
    coordinator: Arc<GlobalCoordinator>,
    config: Args,
) -> Result<KinesisClient, Box<dyn std::error::Error + Send + Sync>> {
    info!("Creating worker: {}", worker_id);

    // Create AWS config
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    // Create the processor
    let processor = Arc::new(MultiWorkerProcessor::new(worker_id.clone(), coordinator.clone()));

    // Register worker with coordinator
    coordinator.register_worker(processor.worker_stats.clone()).await;

    // Update worker status
    {
        let mut status = processor.worker_stats.status.write().await;
        *status = WorkerStatus::Running;
    }

    // Build Kinesis client configuration
    let kinesis_config = KinesisClientConfig::builder()
        .stream_name(config.stream_name)
        .application_name(format!("{}-{}", config.app_name, worker_id))
        .table_name(config.table_name)
        .aws_config(aws_config)
        .management_config(
            ManagementConfig::builder()
                .start_strategy(StartStrategy::Latest)
                .leases_to_acquire(config.leases_to_acquire)
                .max_leases(config.max_leases)
                .build(),
        )
        .lease_config(LeaseConfig::PollingConfig { 
            limit: config.batch_size 
        })
        .build();

    // Create the client
    let factory = KinesisClientFactory::new(kinesis_config);
    let client = factory.create_client(processor).await?;

    Ok(client)
}

async fn worker_supervisor(
    worker_id: String,
    coordinator: Arc<GlobalCoordinator>,
    config: Args,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let restart_delay = Duration::from_secs(config.restart_delay_seconds);
    let auto_restart = config.auto_restart;
    
    loop {
        info!("Starting worker: {}", worker_id);
        
        let client = match create_worker(worker_id.clone(), coordinator.clone(), config.clone()).await {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to create worker {}: {:?}", worker_id, e);
                if auto_restart {
                    info!("Restarting worker {} in {:?}", worker_id, restart_delay);
                    sleep(restart_delay).await;
                    continue;
                } else {
                    break;
                }
            }
        };

        // Start the client
        let client_clone = client.clone();
        let worker_task = tokio::spawn(async move {
            client_clone.start().await
        });

        // Wait for shutdown signal or worker completion
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received for worker {}", worker_id);
                client.shutdown().await;
                break;
            }
            result = worker_task => {
                match result {
                    Ok(Ok(_)) => {
                        info!("Worker {} completed successfully", worker_id);
                        break;
                    }
                    Ok(Err(e)) => {
                        error!("Worker {} failed: {:?}", worker_id, e);
                        if auto_restart {
                            info!("Restarting worker {} in {:?}", worker_id, restart_delay);
                            sleep(restart_delay).await;
                            continue;
                        } else {
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Worker {} task panicked: {:?}", worker_id, e);
                        if auto_restart {
                            info!("Restarting worker {} in {:?}", worker_id, restart_delay);
                            sleep(restart_delay).await;
                            continue;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    // Unregister worker
    coordinator.unregister_worker(&worker_id).await;
    info!("Worker supervisor {} stopped", worker_id);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info,disney_kinesis_client=debug")
        .init();

    let args = Args::parse();

    info!("Starting multi-worker consumer with configuration:");
    info!("  Stream: {}", args.stream_name);
    info!("  Table: {}", args.table_name);
    info!("  Workers: {}", args.workers);
    info!("  Batch Size: {}", args.batch_size);
    info!("  Run Duration: {}s", args.run_duration_seconds);
    info!("  Auto Restart: {}", args.auto_restart);
    info!("  Dynamic Scaling: {}", args.dynamic_scaling);

    // Create global coordinator
    let coordinator = Arc::new(GlobalCoordinator::new(args.target_rps_per_worker));

    // Create shutdown channels
    let (shutdown_tx, _) = mpsc::channel::<()>(args.workers);
    let mut worker_shutdown_txs = Vec::new();

    // Start worker supervisors
    let mut worker_handles = Vec::new();
    for i in 0..args.workers {
        let worker_id = format!("worker-{}", Uuid::new_v4().to_string()[..8]);
        let (worker_shutdown_tx, worker_shutdown_rx) = mpsc::channel::<()>(1);
        worker_shutdown_txs.push(worker_shutdown_tx);

        let coordinator_clone = coordinator.clone();
        let config_clone = args.clone();
        
        let handle = tokio::spawn(async move {
            worker_supervisor(worker_id, coordinator_clone, config_clone, worker_shutdown_rx).await;
        });
        
        worker_handles.push(handle);
    }

    // Start statistics reporting
    let stats_coordinator = coordinator.clone();
    let stats_interval = Duration::from_secs(args.stats_interval_seconds);
    let stats_task = tokio::spawn(async move {
        let mut interval = interval(stats_interval);
        loop {
            interval.tick().await;
            stats_coordinator.print_statistics().await;
        }
    });

    // Dynamic scaling task
    let scaling_task = if args.dynamic_scaling {
        let scaling_coordinator = coordinator.clone();
        Some(tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                
                if scaling_coordinator.should_scale_up().await {
                    info!("Scaling recommendation: SCALE UP");
                    // In a real implementation, you would add more workers here
                } else if scaling_coordinator.should_scale_down().await {
                    info!("Scaling recommendation: SCALE DOWN");
                    // In a real implementation, you would remove workers here
                }
            }
        }))
    } else {
        None
    };

    // Wait for completion or shutdown
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    if args.run_duration_seconds > 0 {
        tokio::select! {
            _ = &mut ctrl_c => {
                info!("Received shutdown signal");
            }
            _ = sleep(Duration::from_secs(args.run_duration_seconds)) => {
                info!("Reached maximum run duration");
            }
        }
    } else {
        ctrl_c.await.expect("Failed to listen for Ctrl+C");
        info!("Received shutdown signal");
    }

    // Stop background tasks
    stats_task.abort();
    if let Some(scaling_task) = scaling_task {
        scaling_task.abort();
    }

    // Shutdown all workers
    info!("Shutting down all workers...");
    for shutdown_tx in worker_shutdown_txs {
        let _ = shutdown_tx.send(()).await;
    }

    // Wait for all workers to complete
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Print final statistics
    coordinator.print_statistics().await;

    info!("Multi-worker consumer stopped");
    Ok(())
}