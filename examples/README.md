# Disney Kinesis Client Examples

This directory contains comprehensive examples showing how to use the Disney Kinesis Client library for consuming from Amazon Kinesis streams in Rust.

## Library Overview

The Disney Kinesis Client is a Rust implementation of the Amazon Kinesis Client Library (KCL), compatible with the Java KCLv2. It provides:

- **Distributed Processing**: Automatic shard assignment and load balancing across multiple consumers
- **Fault Tolerance**: Automatic failover and lease management via DynamoDB
- **At-Least-Once Delivery**: Reliable message processing with checkpointing
- **Aggregated Record Support**: Handles both regular and KPL-aggregated records
- **Metrics Integration**: Built-in CloudWatch metrics

### Core Concepts

**ShardRecordProcessor** - The main interface you implement:
```rust
#[async_trait]
pub trait ShardRecordProcessor: Send + Sync {
    async fn process_records(&self, records: Vec<KinesisClientRecord>, millis_behind_latest: i64);
    async fn lease_lost(&self);
    async fn shard_ended(&self);
    async fn shutdown_requested(&self);
}
```

**Checkpointer** - Used to save processing progress:
```rust
// Checkpoint after processing a batch
checkpointer.checkpoint(last_record.extended_sequence_number()).await?;

// Mark shard as completed
checkpointer.end().await?;
```

## Available Examples

### 1. **Basic Consumer** (`basic_consumer.rs`)
The simplest example showing fundamental usage patterns.

**Usage:**
```bash
cargo run --example basic_consumer -- --stream-name my-test-stream --table-name my-app-leases
```

**Features:**
- Simple record processing with logging
- Basic checkpointing strategy
- Graceful shutdown handling
- Command-line argument parsing

**Best for:** Learning the basics, simple applications

### 2. **Advanced Consumer** (`advanced_consumer.rs`)
A production-ready example with sophisticated processing patterns.

**Usage:**
```bash
# Start from latest records
cargo run --example advanced_consumer -- --stream-name my-test-stream --table-name my-app-leases

# Start from specific timestamp with custom settings
cargo run --example advanced_consumer -- \
  --stream-name my-test-stream \
  --table-name my-app-leases \
  --batch-size 500 \
  --checkpoint-interval-seconds 60 \
  --start-from-timestamp 1640995200000
```

**Features:**
- Batched processing with configurable checkpoint intervals
- JSON message parsing with fallback for other formats
- Comprehensive error handling and retry logic
- Metrics collection and periodic reporting
- Different message types processing (user events, system events, errors)
- Configurable start strategies

**Best for:** Production applications with complex processing requirements

### 3. **Multi-Worker Consumer** (`multi_worker.rs`)
Demonstrates distributed consumption with multiple worker instances.

**Usage:**
```bash
# Run 3 workers competing for leases
cargo run --example multi_worker -- \
  --stream-name my-test-stream \
  --table-name my-app-leases \
  --workers 3

# Run for specific duration with 5 workers
cargo run --example multi_worker -- \
  --stream-name my-test-stream \
  --table-name my-app-leases \
  --workers 5 \
  --run-duration-seconds 300
```

**Features:**
- Multiple workers competing for leases
- Distributed lease management and rebalancing
- Global statistics tracking across workers
- Coordinated shutdown
- Lease reassignment demonstration

**Best for:** Understanding distributed processing, testing scaling scenarios

### 4. **Simple Producer** (`simple_producer.rs`)
A producer example for generating test data.

**Usage:**
```bash
# Produce simple text records
cargo run --example simple_producer -- \
  --stream-name my-test-stream \
  --count 100

# Produce JSON records compatible with advanced consumer
cargo run --example simple_producer -- \
  --stream-name my-test-stream \
  --count 1000 \
  --json-format \
  --interval-ms 500
```

**Features:**
- Simple text and JSON record generation
- Rate limiting with configurable intervals
- Error handling and retry logic
- Different message types (user actions, system events, errors)

**Best for:** Generating test data for consumer examples

### 5. **Configuration Examples** (`config_examples.rs`)
Shows different configuration patterns for various deployment scenarios.

**Usage:**
```bash
cargo run --example config_examples
```

**Features:**
- High-throughput configuration
- Batch processing configuration
- Development/testing configuration
- Cost-optimized configuration
- Replay/catchup configuration
- Environment-based configuration

**Best for:** Understanding configuration options and deployment patterns

## Quick Start Guide

### Step 1: Create a Kinesis Stream
```bash
aws kinesis create-stream --stream-name test-stream --shard-count 4

# Wait for stream to be active
aws kinesis describe-stream --stream-name test-stream
```

### Step 2: Generate Test Data
```bash
cargo run --example simple_producer -- \
  --stream-name test-stream \
  --count 1000 \
  --json-format
```

### Step 3: Start Consumer
```bash
# Basic consumer
cargo run --example basic_consumer -- \
  --stream-name test-stream \
  --table-name test-app-leases

# Or advanced consumer with metrics
cargo run --example advanced_consumer -- \
  --stream-name test-stream \
  --table-name test-app-leases

# Or multi-worker setup
cargo run --example multi_worker -- \
  --stream-name test-stream \
  --table-name test-app-leases \
  --workers 2
```

## Prerequisites

### AWS Setup
1. **AWS Credentials**: Configure via AWS CLI, environment variables, or IAM roles
2. **AWS Permissions**: Your credentials need permissions for Kinesis, DynamoDB, and CloudWatch
3. **Kinesis Stream**: Create a stream or use an existing one

### Required Permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:DescribeStream",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListShards"
            ],
            "Resource": "arn:aws:kinesis:*:*:stream/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:DescribeTable",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:Scan",
                "dynamodb:Query"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/*"
        },
        {
            "Effect": "Allow",
            "Action": "cloudwatch:PutMetricData",
            "Resource": "*"
        }
    ]
}
```

## Key Configuration Options

### ManagementConfig
Controls lease management and scaling behavior:

```rust
ManagementConfig {
    leases_to_acquire: 2,    // Leases to grab when scaling up
    max_leases: 20,          // Maximum leases per worker
    start_strategy: StartStrategy::Latest, // Where to start reading
}
```

**Start Strategies:**
- `StartStrategy::Latest` - Start from most recent records
- `StartStrategy::Oldest` - Start from beginning (TRIM_HORIZON)
- `StartStrategy::Timestamp(millis)` - Start from specific Unix timestamp

### LeaseConfig
Controls record retrieval from Kinesis:

```rust
LeaseConfig::PollingConfig {
    limit: 1000  // Max records per GetRecords call (1-10000)
}
```

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Kinesis       │    │   DynamoDB      │    │   CloudWatch    │
│   Stream        │    │   Lease Table   │    │   Metrics       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                Disney Kinesis Client                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Overseer   │  │   Lease     │  │    Shard Record         │  │
│  │  (Manager)  │  │  Manager    │  │    Processor            │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Your Record   │    │   Checkpointer  │    │   Your Business │
│   Processor     │    │                 │    │   Logic         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Best Practices

### 1. Proper Checkpointing
```rust
// ❌ Don't checkpoint every record (expensive)
for record in records {
    process_record(record).await;
    checkpointer.checkpoint(record.extended_sequence_number()).await;
}

// ✅ Checkpoint periodically or after batches
let mut processed_count = 0;
for record in records {
    process_record(record).await;
    processed_count += 1;
}

// Checkpoint after processing the batch
if let Some(last_record) = records.last() {
    checkpointer.checkpoint(last_record.extended_sequence_number()).await?;
}
```

### 2. Error Handling
```rust
// ✅ Handle errors gracefully without stopping processing
let mut success_count = 0;
let mut error_count = 0;

for record in records {
    match process_record(record).await {
        Ok(_) => success_count += 1,
        Err(e) => {
            error!("Failed to process record: {:?}", e);
            error_count += 1;
            // Continue processing other records
        }
    }
}
```

### 3. Message Type Routing
```rust
#[derive(Deserialize)]
struct Message {
    event_type: String,
    payload: serde_json::Value,
}

async fn process_record(&self, record: &KinesisClientRecord) -> Result<(), Error> {
    let message: Message = serde_json::from_slice(record.record.data.as_ref())?;
    
    match message.event_type.as_str() {
        "user_action" => self.handle_user_action(message).await,
        "system_event" => self.handle_system_event(message).await,
        "error_event" => self.handle_error_event(message).await,
        _ => {
            warn!("Unknown event type: {}", message.event_type);
            Ok(())
        }
    }
}
```

## Monitoring and Troubleshooting

### Built-in Metrics
The library automatically publishes CloudWatch metrics:
- Records processed per shard
- Processing lag (milliseconds behind latest)
- Lease acquisition/loss events
- Checkpoint success/failure rates

### Custom Metrics
```rust
#[derive(Default)]
struct ProcessorMetrics {
    records_processed: AtomicU64,
    processing_errors: AtomicU64,
    last_checkpoint: RwLock<Option<Instant>>,
}

impl ProcessorMetrics {
    async fn report(&self) {
        let processed = self.records_processed.load(Ordering::Relaxed);
        let errors = self.processing_errors.load(Ordering::Relaxed);
        info!("Metrics - Processed: {}, Errors: {}", processed, errors);
    }
}
```

## Troubleshooting

### Common Issues and Solutions

#### High Processing Lag
**Symptoms:** `millis_behind_latest` is consistently high  
**Solutions:**
- Increase number of consumer instances
- Optimize record processing logic
- Increase `limit` in `LeaseConfig` for larger batches
- Check for downstream bottlenecks

#### Lease Competition
**Symptoms:** Workers frequently losing/gaining leases  
**Solutions:**
- Reduce `leases_to_acquire` for less aggressive acquisition
- Ensure sufficient shards for all consumers
- Check DynamoDB table capacity and permissions

#### Duplicate Processing
**Symptoms:** Same records processed multiple times  
**Solutions:**
- Ensure proper checkpointing after successful processing
- Implement idempotent processing logic
- Check for lease timeout issues

#### Memory Usage Growth
**Symptoms:** Memory usage increases over time  
**Solutions:**
- Process records in batches rather than accumulating
- Clear processed data promptly
- Monitor batch sizes and checkpoint frequency

## Environment Variables

Configure examples using environment variables:

```bash
# AWS Configuration
export AWS_REGION=us-east-1
export AWS_PROFILE=my-profile

# Kinesis Configuration  
export KINESIS_LEASES_TO_ACQUIRE=2
export KINESIS_MAX_LEASES=20
export KINESIS_START_STRATEGY=LATEST  # or OLDEST, or TIMESTAMP:1640995200000
export KINESIS_BATCH_LIMIT=1000

# Logging
export RUST_LOG=info,disney_kinesis_client=debug
```

## Dependencies

The examples use these dependencies (add to your `Cargo.toml`):

```toml
[dependencies]
disney-kinesis-client = "0.1.0"
tokio = { version = "1", features = ["full"] }
tracing = "0.1"
async-trait = "0.1"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
uuid = { version = "1.0", features = ["v4"] }
aws-config = { version = "1.0", features = ["behavior-version-latest"] }
aws-sdk-kinesis = "1.0"
aws-sdk-dynamodb = "1.0"
aws-sdk-cloudwatch = "1.0"

[dev-dependencies]
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
clap = { version = "4.0", features = ["derive"] }
```

## Next Steps

1. **Start with the basic consumer**: Get familiar with the core concepts
2. **Experiment with configurations**: Try different settings to understand their impact
3. **Scale up**: Test with multiple workers and higher throughput
4. **Add monitoring**: Implement custom metrics for your use case
5. **Deploy to production**: Use the patterns from the advanced example

## Support

- **Source code**: [GitHub Repository](https://github.com/disney/disney-kinesis-client-rust)
- **AWS Kinesis docs**: [Amazon Kinesis Documentation](https://docs.aws.amazon.com/kinesis/)
- **Java KCL reference**: [Kinesis Client Library Documentation](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html)
