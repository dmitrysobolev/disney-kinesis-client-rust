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

// tests of single and multiple consumers on the test stream (that must be
// populated with populate.rs before running)
use std::{sync::Arc, time::Duration};

use async_rwlock::RwLock;
use disney_kinesis_client::*;
use itertools::Itertools;
use tokio::time::sleep;

mod common;
use common::*;

// cargo test single_worker -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn single_worker() {
    let (cw, ddb, kinesis) = init().await;

    let stream_name = test_stream_name();
    let table_name = stream_name.clone() + "-single";

    drop_table(&ddb, &table_name).await;

    let cw = Arc::new(cw);
    let ddb = Arc::new(ddb);
    let kinesis = Arc::new(kinesis);

    let management = ManagementConfig {
        leases_to_acquire: 16,
        max_leases: 64,
        start_strategy: StartStrategy::Oldest,
    };
    let config = LeaseConfig::PollingConfig { limit: 1000 }; // to slow down the test

    let callback = Arc::new(CountingWorker {
        entries: Arc::new(RwLock::new(Vec::new())),
    });
    let consumer = Client::new(
        callback.clone(),
        cw,
        ddb,
        kinesis,
        table_name,
        stream_name,
        management,
        config,
    )
    .await;

    let consumer = consumer.run().await.unwrap(); // test code

    let mut entries: usize = 0;
    loop {
        sleep(Duration::from_secs(20)).await;
        let records = &callback.entries.read().await;
        if records.len() == entries {
            break;
        }
        entries = records.len();
    }

    consumer.shutdown().await;

    let records = &callback.entries.read().await;
    let record_count = records.iter().count();
    let unique_count = records.iter().unique().collect_vec().len();
    tracing::info!("RESULT: got {record_count} entries, {unique_count} unique");

    assert!(unique_count == TEST_UNIQUE_RECORDS);
    // very tight requirement because there's no stealing going on
    assert!(record_count < TEST_UNIQUE_RECORDS * 101 / 100)
}

// cargo test multi_workers -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn multi_workers() {
    let (cw, ddb, kinesis) = init().await;

    let stream_name = test_stream_name();
    let table_name = stream_name.clone() + "-multi";

    drop_table(&ddb, &table_name).await;

    let cw = Arc::new(cw);
    let ddb = Arc::new(ddb);
    let kinesis = Arc::new(kinesis);
    let config = LeaseConfig::PollingConfig { limit: 1000 }; // slow down the test
    let management = ManagementConfig {
        leases_to_acquire: 2, // low to minimise in-fighting
        max_leases: 64,
        start_strategy: StartStrategy::Oldest,
    };

    // same callback for all the workers
    let callback = Arc::new(CountingWorker {
        entries: Arc::new(RwLock::new(Vec::new())),
    });

    let consumer1 = Client::new(
        callback.clone(),
        cw.clone(),
        ddb.clone(),
        kinesis.clone(),
        table_name.clone(),
        stream_name.clone(),
        management.clone(),
        config.clone(),
    )
    .await;
    let consumer2 = Client::new(
        callback.clone(),
        cw.clone(),
        ddb.clone(),
        kinesis.clone(),
        table_name.clone(),
        stream_name.clone(),
        management.clone(),
        config.clone(),
    )
    .await;

    let consumer1 = consumer1.run().await.unwrap(); // test code
    sleep(Duration::from_secs(5)).await; // jitter to avoid collisions
    let consumer2 = consumer2.run().await.unwrap(); // test code

    let mut entries: usize = 0;
    loop {
        sleep(Duration::from_secs(20)).await;
        let records = &callback.entries.read().await;
        if records.len() == entries {
            break;
        }
        entries = records.len();
    }

    consumer1.shutdown().await;
    consumer2.shutdown().await;

    let records = &callback.entries.read().await;
    let record_count = records.iter().count();
    let unique_count = records.iter().unique().collect_vec().len();
    tracing::info!("RESULT: got {record_count} entries, {unique_count} unique");

    assert!(unique_count == TEST_UNIQUE_RECORDS);
    // still high standards, but we do expect some lease stealing
    assert!(record_count < TEST_UNIQUE_RECORDS * 105 / 100)
}

// Local Variables:
// compile-command: "cargo fmt && cargo test --no-run"
// End:
