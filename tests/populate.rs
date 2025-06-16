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

// Tears down and populates a test kinesis stream, then creates shards and fills
// them with records, including scaling out and scaling back in. Consumers
// (including multi-process and multi-lang) can make assertions about the state
// of their lease table and persistent state at the end of the run.
//
// To provision a kinesis stream, run
//
//   cargo test populate_stream -- --ignored --nocapture
//
use std::time::{Duration, Instant};

use aws_sdk_kinesis::{
    primitives::Blob,
    types::{PutRecordsRequestEntry, ScalingType, StreamMode, StreamModeDetails},
};
use disney_kinesis_client::aggregated::*;
use tokio::time::{sleep, sleep_until};
use uuid::Uuid;

mod common;
use common::*;

async fn setup_stream(kinesis: &aws_sdk_kinesis::Client) {
    let name = test_stream_name();
    drop_stream(&kinesis, &name).await;

    let mut shards: i32 = 16;

    tracing::info!("CREATING PROVISIONED STREAM {name} WITH {shards} SHARDS");
    kinesis
        .create_stream()
        .stream_name(&name)
        .shard_count(shards)
        .stream_mode_details(
            StreamModeDetails::builder()
                .stream_mode(StreamMode::Provisioned)
                .build()
                .unwrap(), // test code
        )
        .send()
        .await
        .unwrap(); // test code

    use aws_sdk_kinesis::client::Waiters;
    kinesis
        .wait_until_stream_exists()
        .stream_name(&name)
        .wait(Duration::from_secs(10))
        .await
        .unwrap(); // test code

    tracing::debug!("POPULATING THE KINESIS STREAM {name}");

    let mut i: usize = 0;
    while i < TEST_UNIQUE_RECORDS {
        let loop_start = Instant::now();

        // 1% chance of a scaling event
        if rand::random::<f64>() < 0.01 {
            if shards == 2 || shards < 64 && rand::random() {
                // scale out
                shards = shards * 2;
                tracing::debug!("SCALING OUT THE KINESIS STREAM {name} TO {shards} SHARDS");
            } else {
                // scale n
                shards = shards / 2;
                tracing::debug!("SCALING IN THE KINESIS STREAM {name} TO {shards} SHARDS");
            }

            // this can fail if we do it too often, so just ignore when that
            // happens. unfortunately Kinesis can be slow to action a rescale.
            let result = kinesis
                .update_shard_count()
                .stream_name(&name)
                .target_shard_count(shards)
                .scaling_type(ScalingType::UniformScaling)
                .send()
                .await;
            if result.is_err() {
                tracing::info!("SCALING FAILED IN THE KINESIS STREAM {name}");
            }
        }

        let mut batch = Vec::new();

        // 10% chance we'll use aggregated records
        if rand::random::<f64>() < 0.1 {
            let common_partition_key = Uuid::new_v4().to_string();
            let mut records = Vec::new();
            for _ in 0..100 {
                records.push(Record {
                    partition_key_index: 0,
                    explicit_hash_key_index: None,
                    data: Uuid::new_v4().to_string().into(),
                });
            }

            let data = aggregate(&AggregatedRecord {
                partition_key_table: vec![common_partition_key.clone()],
                explicit_hash_key_table: Vec::new(),
                records,
            });

            // we should really use an explicit_hash_key here to more closely
            // mimick the KPL, but this is still perfectly valid.
            // explicit_hash_key is only really useful if you want to target a
            // specific shard.
            batch.push(
                PutRecordsRequestEntry::builder()
                    .data(Blob::new(data))
                    .partition_key(common_partition_key)
                    .build()
                    .unwrap(), // test code
            );
            i += 100;
        } else {
            for _ in 0..100 {
                // Minimum number of 1 item. Maximum number of 500 items.
                let content = Uuid::new_v4().to_string();
                batch.push(
                    PutRecordsRequestEntry::builder()
                        .data(Blob::new(content.clone()))
                        .partition_key(content)
                        .build()
                        .unwrap(), // test code
                );
                i += 1;
            }
        }

        let putter = kinesis
            .put_records()
            .stream_name(&name)
            .set_records(Option::Some(batch));

        let mut success = false;
        let mut attempts = 0;
        while !success {
            // annoying that we have to clone the data to be able to retry
            // https://github.com/awslabs/aws-sdk-rust/issues/1218
            let results = putter.clone().send().await.unwrap(); // test code
            let failures = results.failed_record_count;
            if failures.unwrap_or(0) == 0 {
                success = true;
            } else {
                tracing::debug!("RETRYING A WRITE TO THE KINESIS STREAM {name}");
                // we may have dupe records now, how exciting! it is possible to
                // only resend the ones that failed, but it's not worth it here.
                attempts += 1;
                sleep(Duration::from_secs(1)).await;
                if attempts >= 10 {
                    panic!("Too many retry attempts writing to kinesis stream {name}");
                }
            }
        }

        tracing::info!("WRITTEN {i} RECORDS TO THE KINESIS STREAM {name}");

        // Each shard can support writes up to 1,000 records per second, up to a
        // maximum data write total of 1 MiB per second. For small messages,
        // this is 10 batches of 100 per second per shard, but we'll stay well
        // away from that limit by using it as a whole stream limit.
        sleep_until((loop_start + Duration::from_millis(100)).into()).await;
    }
}

async fn drop_stream(client: &aws_sdk_kinesis::Client, stream_name: &String) {
    use aws_sdk_kinesis::client::Waiters;

    match client.delete_stream().stream_name(stream_name).send().await {
        Ok(_) => tracing::info!("DROPPED KINESIS STREAM {stream_name}"),
        Err(e) => {
            let e = e.into_service_error();
            if !e.is_resource_not_found_exception() {
                panic!("Problem deleting the kinesis stream {e:?}")
            }
        }
    }
    client
        .wait_until_stream_not_exists()
        .stream_name(stream_name)
        .wait(Duration::from_secs(30))
        .await
        .unwrap(); // test code
}

#[tokio::test]
#[ignore]
async fn populate_stream() {
    let (_cw, _ddb, kinesis) = init().await;
    setup_stream(&kinesis).await;
}

// Local Variables:
// compile-command: "cargo fmt && cargo test --no-run"
// End:
