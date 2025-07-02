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

// common functions for integration tests
use std::{env, sync::Arc, time::Duration};

use async_rwlock::RwLock;
use async_trait::async_trait;
use disney_kinesis_client::{
    Checkpointer, KinesisClientRecord, ShardRecordProcessor, ShardRecordProcessorFactory,
};

// the number of unique record payloads that are inserted into the stream
pub const TEST_UNIQUE_RECORDS: usize = 100000;

pub fn test_stream_name() -> String {
    env::var("TEST_KINESIS_STREAM").unwrap_or("p2opsprincipals-disney-kcl-rust".to_string())
}

pub async fn init() -> (
    aws_sdk_cloudwatch::Client,
    aws_sdk_dynamodb::Client,
    aws_sdk_kinesis::Client,
) {
    // tracing_subscriber::fmt().json().init();
    tracing_subscriber::fmt().init();

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::v2025_01_17()).await;

    let cw = aws_sdk_cloudwatch::Client::new(&config);
    let ddb = aws_sdk_dynamodb::Client::new(&config);
    let kinesis = aws_sdk_kinesis::Client::new(&config);
    (cw, ddb, kinesis)
}

// not used in all tests, which confuses the linter...
#[allow(dead_code)]
pub async fn drop_table(client: &aws_sdk_dynamodb::Client, table_name: &String) {
    use aws_sdk_dynamodb::client::Waiters;

    match client.delete_table().table_name(table_name).send().await {
        Ok(_) => tracing::info!("DROPPED DDB LEASE TABLE {table_name}"),
        Err(e) => {
            let e = e.into_service_error();
            if !e.is_resource_not_found_exception() {
                panic!("Problem deleting the ddb table {e:?}")
            }
        }
    };
    client
        .wait_until_table_not_exists()
        .table_name(table_name)
        .wait(Duration::from_secs(30))
        .await
        .unwrap(); // test code
}

#[derive(Clone)]
pub struct CountingWorker {
    // shared with the shard processors...
    pub entries: Arc<RwLock<Vec<String>>>,
}
#[derive(Clone)]
struct CountingShardProcessor {
    checkpointer: Checkpointer,
    entries: Arc<RwLock<Vec<String>>>,
}

#[async_trait]
impl ShardRecordProcessorFactory for CountingWorker {
    async fn initialize(&self, checkpointer: Checkpointer) -> Box<dyn ShardRecordProcessor> {
        return Box::new(CountingShardProcessor {
            checkpointer: checkpointer,
            entries: self.entries.clone(),
        });
    }
}

#[async_trait]
impl ShardRecordProcessor for CountingShardProcessor {
    async fn process_records(&self, records: Vec<KinesisClientRecord>, _millis_behind_latest: i64) {
        let entries = &mut *self.entries.write().await;
        let checkpoint = records.last().unwrap().extended_sequence_number(); // test code

        records.into_iter().for_each(|e| {
            let bytes = e.record.data.into_inner();
            let content = String::from_utf8(bytes).unwrap(); // test code
            entries.push(content);
        });
        self.checkpointer.checkpoint(checkpoint).await.unwrap(); // test code
    }

    async fn lease_lost(&self) {}
    async fn shard_ended(&self) {
        let _ = self.checkpointer.end().await.unwrap(); // test code
    }
    async fn shutdown_requested(&self) {}
}

// Local Variables:
// compile-command: "cargo fmt && cargo test --no-run"
// End:
