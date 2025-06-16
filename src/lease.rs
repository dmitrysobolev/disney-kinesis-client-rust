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

use aws_sdk_cloudwatch::types::MetricDatum;
// logic for managing a single lease
use tokio_util::sync::CancellationToken;

use std::{
    cmp::max,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant, SystemTime},
};

use crate::{
    oversee::HEARTBEAT_TIMEOUT,
    types::{
        Lease,
        SequenceNumber::{AT_TIMESTAMP, LATEST, SHARD_END, TRIM_HORIZON},
    },
    ExtendedSequenceNumber, Fail, KinesisClientRecord, LeaseConfig, ShardRecordProcessorFactory,
};
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_kinesis::{primitives::DateTime, types::ShardIteratorType};
use itertools::Itertools;
use tokio::{sync::mpsc::UnboundedSender, time::sleep_until};

// from a user-facing perspective this is only capable of checkpointing, but
// internally it is used to handle a variety of tasks on the current lease.
/// Made available to the user to checkpoint and end leases on shards.
#[derive(Clone)]
pub struct Checkpointer {
    cw_client: Arc<aws_sdk_cloudwatch::client::Client>,
    ddb_client: Arc<aws_sdk_dynamodb::client::Client>,
    pub table_name: String,
    pub stream_name: String,
    pub worker_identifier: String,
    pub lease_key: String,
    sender_overseer: UnboundedSender<String>,
    request_heartbeat: Arc<AtomicBool>,
}

// The top level task that should be spawned for new leases.
// This will run until the lease is lost, ended, or shutdown.
pub(crate) async fn start(
    factory: Arc<dyn ShardRecordProcessorFactory>,
    cw_client: Arc<aws_sdk_cloudwatch::client::Client>,
    ddb_client: Arc<aws_sdk_dynamodb::client::Client>,
    kinesis_client: Arc<aws_sdk_kinesis::client::Client>,
    sender_overseer: UnboundedSender<String>,
    shutdown: CancellationToken,
    table_name: String,
    stream_name: String,
    worker_identifier: String,
    lease_config: LeaseConfig,
    lease: Lease,
) -> Result<(), Fail> {
    let lease_key = lease.lease_key.clone();
    tracing::info!(
        worker = worker_identifier,
        table = table_name,
        stream = stream_name,
        lease = lease_key,
        "acquiring lease",
    );

    let worker = Checkpointer {
        cw_client: cw_client,
        ddb_client: ddb_client.clone(),
        table_name: table_name.clone(),
        stream_name: stream_name.clone(),
        worker_identifier: worker_identifier.clone(),
        lease_key: lease.lease_key.clone(),
        sender_overseer,
        request_heartbeat: Arc::new(AtomicBool::new(false)),
    };
    let acquired = worker.acquire(&lease).await?;

    if !acquired {
        tracing::info!(
            worker = worker_identifier,
            table = table_name,
            stream = stream_name,
            lease = lease_key,
            "failed to acquire the lease"
        );
        // could log that somebody beat us to it, but maybe better as a custom metric
        return Ok(());
    }

    tracing::debug!(
        worker = worker_identifier,
        table = table_name,
        stream = stream_name,
        lease = lease_key,
        "acquired the lease"
    );
    let mut heartbeat_due = Instant::now() + HEARTBEAT_TIMEOUT;

    // annoying that we have to do the name => arn lookup for the polling api
    let stream_arn = kinesis_client
        .describe_stream_summary()
        .stream_name(&stream_name)
        .send()
        .await?
        .stream_description_summary
        .unwrap() // Option seems like a bug in the smithy SDK
        .stream_arn;

    // if the leaseOwner was somebody else, we wait the heartbeat period,
    // recheck the checkpoint, so that we give the old owner some grace time to
    // finish up. Removing this optimisation leads to a big increase in
    // duplicated records in our tests.
    let starting_checkpoint = if lease.lease_owner.is_none() {
        lease.checkpoint
    } else {
        // we will be a little bit late with our heartbeat, but it's ok
        sleep_until(heartbeat_due.into()).await;
        let update = crate::get_lease(&ddb_client, &table_name, &lease_key).await?;
        update.map(|e| e.checkpoint).unwrap_or(lease.checkpoint)
    };

    // https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
    let mut get_iterator = kinesis_client
        .get_shard_iterator()
        .stream_name(&stream_name)
        .shard_id(&lease_key);

    if starting_checkpoint.is_trim_horizon() {
        get_iterator = get_iterator.shard_iterator_type(ShardIteratorType::TrimHorizon);
    } else if starting_checkpoint.is_latest() {
        get_iterator = get_iterator.shard_iterator_type(ShardIteratorType::Latest);
    } else if starting_checkpoint.is_at_timestamp() {
        get_iterator = get_iterator
            .shard_iterator_type(ShardIteratorType::AtTimestamp)
            .timestamp(DateTime::from_millis(
                starting_checkpoint.sub_sequence_number,
            ));
    } else {
        // the Java KCL uses AT in its first call, so that it never misses a
        // sub_sequence_number for an aggregated message, which means we almost
        // always have to throw away the first record.
        get_iterator = get_iterator
            .starting_sequence_number(&starting_checkpoint.sequence_number.to_string())
            .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
    }

    let mut iterator = get_iterator.send().await?.shard_iterator;

    let mut first_run = true;

    let callback = factory.initialize(worker.clone()).await;

    let mut millis_behind_latest: i64 = 0;
    let mut bytes: usize = 0;
    let mut record_count: usize = 0;

    // in a loop, poll for records and send to the user, then repeat. The
    // user is responsible for checkpointing.
    loop {
        if shutdown.is_cancelled() {
            tracing::debug!(
                worker = worker_identifier,
                table = table_name,
                stream = stream_name,
                lease = lease_key,
                "lease stopping on request"
            );
            callback.shutdown_requested().await;
            worker.release().await?;
            return Ok(());
        }

        let loop_started = Instant::now();
        if heartbeat_due <= loop_started || worker.heartbeat_requested() {
            // we could spawn this whole block if it is limiting throughput
            tracing::debug!(
                worker = worker_identifier,
                table = table_name,
                stream = stream_name,
                lease = lease_key,
                "heartbeating"
            );
            let updated = worker.heartbeat().await?;

            worker
                .publish_metrics(millis_behind_latest, bytes, record_count)
                .await?;
            millis_behind_latest = 0;
            bytes = 0;
            record_count = 0;

            if !updated {
                if iterator.is_some() {
                    tracing::info!(
                        worker = worker_identifier,
                        table = table_name,
                        stream = stream_name,
                        lease = lease_key,
                        "lease was taken"
                    );
                    callback.lease_lost().await;
                }
                return Ok(());
            }
            heartbeat_due = loop_started + HEARTBEAT_TIMEOUT;
        }

        if iterator.is_none() {
            callback.shard_ended().await;

            // continue heartbeating until the user checkpoints the end.
            sleep_until(heartbeat_due.into()).await;
            continue;
        }

        let limit = match lease_config {
            LeaseConfig::PollingConfig { limit } => limit,
        };

        // https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html
        let batch = kinesis_client
            .get_records()
            .limit(limit)
            .stream_arn(&stream_arn)
            .set_shard_iterator(iterator)
            .send()
            .await?;

        iterator = batch.next_shard_iterator;
        let mut records = batch
            .records
            .into_iter()
            .flat_map(KinesisClientRecord::from)
            .collect_vec();

        if first_run {
            first_run = false;
            let starting_checkpoint_number = starting_checkpoint.sequence_number.to_string();
            records.retain(|e| !{
                e.record.sequence_number == starting_checkpoint_number && {
                    !e.aggregated
                        || e.sub_sequence_number <= starting_checkpoint.sub_sequence_number
                }
            });
        }

        // the callback will consume the records
        let had_records = !records.is_empty();
        if had_records {
            let mbl = batch.millis_behind_latest.unwrap_or(0);
            millis_behind_latest = max(millis_behind_latest, mbl);
            records.iter().for_each(|r| {
                bytes += r.record.data.as_ref().len();
                record_count += 1;
            });

            callback.process_records(records, mbl).await;
        }

        if iterator.is_none() {
            continue;
        }
        // GetRecords has a limit of five transactions per second per shard,
        // this ensures we respect those limits at all times. Note that this
        // implies a 50,000 (unaggregated) records per shard per second with
        // polling fetches. Each data record can be up to 1 MiB in size, and
        // each shard can read up to 2 MiB per second (users can add
        // additional self-throttle to account for that limit).
        let self_throttle = loop_started + Duration::from_millis(200);
        sleep_until(self_throttle.into()).await;

        if had_records {
            continue;
        }

        // when there were no records, wait a bit longer (impacts latency
        // expectations so we may wish to make this configurable).
        let poll_due = loop_started + Duration::from_secs(2);
        let wakey_time = poll_due.min(heartbeat_due);
        sleep_until(wakey_time.into()).await;
    }
}

impl Checkpointer {
    // every time we think we can start processing a lease, this is spawned to
    // acquire the lease, returning true if we succeeded and false if we hit a
    // race condition and somebody got it before us. All other errors are
    // captured in the Fail.
    async fn acquire(&self, lease: &Lease) -> Result<bool, Fail> {
        // https://dynobase.dev/dynamodb-rust/#update-item

        let one = AttributeValue::N("1".to_string()); // le sigh
        let new_owner = AttributeValue::S(self.worker_identifier.clone());
        let last_owner = if let Some(owner) = &lease.lease_owner {
            AttributeValue::S(owner.clone())
        } else {
            AttributeValue::Null(true) // incase another KCL nulled it out
        };

        let result = self
            .ddb_client
            .update_item()
            .table_name(self.table_name.clone())
            .key("leaseKey", AttributeValue::S(self.lease_key.clone()))
            .update_expression(
                "SET leaseOwner = :new_owner, leaseCounter = :one \
                 ADD ownerSwitchesSinceCheckpoint :one \
                 REMOVE checkpointOwner, pendingCheckpoint, pendingCheckpointSubSequenceNumber, pendingCheckpointState, childShardIds, throughputKBps",
            )
            .expression_attribute_values(":one", one)
            .expression_attribute_values(":new_owner", new_owner)
            .condition_expression("attribute_not_exists(leaseOwner) OR leaseOwner = :last_owner")
            .expression_attribute_values(":last_owner", last_owner)
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => match e.as_service_error() {
                Some(e) if e.is_conditional_check_failed_exception() => Ok(false),
                _ => Err(From::from(e)),
            },
        }
    }

    async fn release(&self) -> Result<bool, Fail> {
        let result = self
            .ddb_client
            .update_item()
            .table_name(self.table_name.clone())
            .key("leaseKey", AttributeValue::S(self.lease_key.clone()))
            .update_expression(
                "SET leaseCounter = :zero \
                 REMOVE leaseOwner",
            )
            .expression_attribute_values(":zero", AttributeValue::N("0".to_string()))
            .condition_expression("leaseOwner = :owner")
            .expression_attribute_values(
                ":owner",
                AttributeValue::S(self.worker_identifier.clone()),
            )
            .send()
            .await;

        match result {
            Ok(_) => {
                tracing::debug!(
                    worker = self.worker_identifier,
                    table = self.table_name,
                    stream = self.stream_name,
                    lease = self.lease_key,
                    "released the lease",
                );
                Ok(true)
            }
            Err(e) => match e.as_service_error() {
                Some(e) if e.is_conditional_check_failed_exception() => Ok(false),
                _ => Err(From::from(e)),
            },
        }
    }

    // true if we succeeded, false if we lost ownership (or it ended), Fail for everything else
    async fn heartbeat(&self) -> Result<bool, Fail> {
        let one = AttributeValue::N("1".to_string()); // le sigh
        let result = self
            .ddb_client
            .update_item()
            .table_name(self.table_name.clone())
            .key("leaseKey", AttributeValue::S(self.lease_key.clone()))
            .update_expression("ADD leaseCounter :one")
            .expression_attribute_values(":one", one)
            .condition_expression("leaseOwner = :owner AND checkpoint <> :ended")
            .expression_attribute_values(
                ":owner",
                AttributeValue::S(self.worker_identifier.clone()),
            )
            .expression_attribute_values(":ended", AttributeValue::S(SHARD_END.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => match e.as_service_error() {
                Some(e) if e.is_conditional_check_failed_exception() => Ok(false),
                _ => Err(From::from(e)),
            },
        }
    }

    // these are potentially very chatty for very large streams (e.g. 1,000+
    // shards) so we might want to think about sending them back up to the
    // overseer to publish larger batches.
    //
    // As of December 2024 (https://aws.amazon.com/cloudwatch/pricing/) a
    // PutMetrics call is charged at $0.01 per 1,000 requests, which is roughly
    // $14 / day for this method on a 1,000 shard stream, so such an
    // optimisation would be worth $5k / year... a rounding error for a stream
    // so big.
    async fn publish_metrics(
        &self,
        millis_behind_latest: i64,
        bytes: usize,
        records: usize,
    ) -> Result<(), Fail> {
        tracing::debug!(
            worker = self.worker_identifier,
            table = self.table_name,
            stream = self.stream_name,
            lease = self.lease_key,
            "millis_behind_latest={millis_behind_latest}, \
             bytes={bytes}, \
             records={records}"
        );

        let nowish = DateTime::from(SystemTime::now());

        let millis_behind_latest = MetricDatum::builder()
            .metric_name("millis_behind_latest")
            .value(millis_behind_latest as f64)
            .timestamp(nowish.clone())
            .build();
        let bytes = MetricDatum::builder()
            .metric_name("bytes")
            .value(bytes as f64)
            .timestamp(nowish.clone())
            .build();
        let records = MetricDatum::builder()
            .metric_name("records")
            .value(records as f64)
            .timestamp(nowish.clone())
            .build();

        self.cw_client
            .put_metric_data()
            .namespace(&self.table_name)
            .metric_data(millis_behind_latest)
            .metric_data(bytes)
            .metric_data(records)
            .send()
            .await?;

        Ok(())
    }

    /// Writes a tombstone checkpoint into the lease table that indicates that
    /// the shard has been fully consumed.
    ///
    /// Returns true if the checkpoint succeeded, and false if it was too stale
    /// to be written. In either case, the lease is released.
    pub async fn end(&self) -> Result<bool, Fail> {
        let checkpoint = ExtendedSequenceNumber {
            sequence_number: SHARD_END,
            sub_sequence_number: 0,
        };
        let result = self.checkpoint(checkpoint).await?;
        if result {
            // successfully ended, prioritise leasing the children
            let _ = self.sender_overseer.send(self.lease_key.clone());
            tracing::info!(
                worker = self.worker_identifier,
                table = self.table_name,
                stream = self.stream_name,
                lease = self.lease_key,
                "ended the lease",
            );
        } else {
            // this will cause the next heartbeat to fail and end the task
            self.release().await?;
        }
        return Ok(result);
    }

    /// Writes a checkpoint into the lease table indicating the most recent
    /// record in this shard to have been fully processed.
    ///
    /// Returns true if the checkpoint succeeded, and false if it was too stale
    /// to be written (and indicating that work duplication has occurred).
    ///
    /// This worker does not need to own the lease for the checkpoint to
    /// succeed, allowing checkpoints to still be written after the point that
    /// another worker has taken the lease.
    pub async fn checkpoint(&self, checkpoint: ExtendedSequenceNumber) -> Result<bool, Fail> {
        assert!(!checkpoint.is_latest());
        assert!(!checkpoint.is_trim_horizon());
        assert!(!checkpoint.is_at_timestamp());

        // checkpoints are allowed when
        //
        // - we are ending the lease (even if it is already ended)
        //
        // - the (aggregated) sequence number is higher than before
        //   (lexicographically)
        //
        // - the previous value is a sentinel
        //
        // note that we do not have to own the lease. This allows progress to be
        // made even if leases are thrashing (and doing duplicate work).
        //
        // unfortunately we cannot do a numeric comparison because Java KCL uses
        // the S type for the sequence_number, and we have to remain compatible,
        // using the format "0|([1-9]\d{0,128})". For lexicographical ordering
        // to work we need to check the size of the number. Thankfully we know
        // that nobody can zero pad the numbers because of the implied format
        // definition.
        let mut query = self
            .ddb_client
            .update_item()
            .table_name(self.table_name.clone())
            .key("leaseKey", AttributeValue::S(self.lease_key.clone()))
            .expression_attribute_values(":SHARD_END", AttributeValue::S(SHARD_END.to_string()))
            .expression_attribute_values(":zero", AttributeValue::N("0".to_string()));

        if checkpoint.is_ended() {
            // no condition, always allowed
            query = query.update_expression(
                "SET checkpoint = :SHARD_END, ownerSwitchesSinceCheckpoint = :zero \
                 REMOVE leaseOwner",
            );
        } else {
            query = query.update_expression(
                "SET checkpoint = :sequence_number, ownerSwitchesSinceCheckpoint = :zero")
                .condition_expression(
                "checkpoint <> :SHARD_END AND ( \
                      (checkpoint IN (:LATEST, :TRIM_HORIZON, :AT_TIMESTAMP)) \
                   OR (size(checkpoint) < size(:sequence_number) OR size(checkpoint) = size(:sequence_number) AND checkpoint < :sequence_number) \
                   OR (checkpoint = :sequence_number AND checkpointSubSequenceNumber > :sub_sequence_number) \
                 )")
                .expression_attribute_values(":LATEST", AttributeValue::S(LATEST.to_string()))
                .expression_attribute_values(":TRIM_HORIZON", AttributeValue::S(TRIM_HORIZON.to_string()))
                .expression_attribute_values(":AT_TIMESTAMP", AttributeValue::S(AT_TIMESTAMP.to_string()))
                .expression_attribute_values(":sequence_number", AttributeValue::S(checkpoint.sequence_number.to_string()))
                .expression_attribute_values(":sub_sequence_number", AttributeValue::N(checkpoint.sub_sequence_number.to_string()));
        }
        match query.send().await {
            Ok(_) => Ok(true),
            Err(e) => match e.as_service_error() {
                Some(e) if e.is_conditional_check_failed_exception() => {
                    tracing::info!(
                        worker = self.worker_identifier,
                        table = self.table_name,
                        stream = self.stream_name,
                        lease = self.lease_key,
                        "checkpoint failed"
                    );
                    self.request_heartbeat();
                    Ok(false)
                }
                _ => Err(From::from(e)),
            },
        }
    }

    fn request_heartbeat(&self) {
        self.request_heartbeat
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn heartbeat_requested(&self) -> bool {
        return self
            .request_heartbeat
            .swap(false, std::sync::atomic::Ordering::Relaxed);
    }
}
