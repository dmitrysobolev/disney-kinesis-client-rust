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

// tests of running a rust consumer and a java consumer at the some time on the
// test stream (which must be populated with populate.rs before running)
use std::{sync::Arc, time::Duration};

use async_process::Command;
use async_rwlock::RwLock;
use disney_kinesis_client::*;
use itertools::Itertools;
use tokio::fs::read_to_string;
use tokio::time::sleep;

mod common;
use common::*;

// cargo test multilang_workers -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn multilang_workers() {
    let (cw, ddb, kinesis) = init().await;

    let stream_name = test_stream_name();
    let table_name = stream_name.clone() + "-multilang";

    drop_table(&ddb, &table_name).await;

    let cw = Arc::new(cw);
    let ddb = Arc::new(ddb);
    let kinesis = Arc::new(kinesis);
    let config = LeaseConfig::PollingConfig { limit: 1000 }; // slow down the test
    let management = ManagementConfig {
        leases_to_acquire: 5, // roughly tuned to play nice with java
        max_leases: 64,
        start_strategy: StartStrategy::Oldest,
    };

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

    let consumer1 = consumer1.run().await.unwrap(); // test code
    sleep(Duration::from_secs(5)).await; // jitter to avoid collisions

    let consumer2 = Command::new("java")
        .arg("-jar")
        .arg("tests/java/java-kcl-worker.jar")
        .arg(stream_name.clone())
        .arg(table_name.clone())
        .spawn()
        .expect("Failed to start the Java KCL worker");

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
    consumer2.output().await.unwrap();

    let mut records2 = Vec::new();

    let output = read_to_string("java-kcl-worker.txt").await.unwrap();
    for line in output.lines() {
        records2.push(line.to_string());
    }

    let records = &*callback.entries.read().await;
    let rust_record_count = records.iter().count();
    let java_record_count = records2.iter().count();
    let total_record_count = rust_record_count + java_record_count;

    let unique_count = records.iter().chain(&records2).unique().collect_vec().len();
    tracing::info!("RESULT: got {total_record_count} entries, {unique_count} unique, {rust_record_count} from rust, {java_record_count} from java");

    assert!(unique_count == TEST_UNIQUE_RECORDS);
    // still high standards, but we do expect some lease stealing
    assert!(java_record_count >= TEST_UNIQUE_RECORDS * 10 / 100);
    assert!(rust_record_count >= TEST_UNIQUE_RECORDS * 10 / 100);
    assert!(total_record_count <= TEST_UNIQUE_RECORDS * 105 / 100);
}

// Local Variables:
// compile-command: "cargo fmt && cargo test --no-run"
// End:
