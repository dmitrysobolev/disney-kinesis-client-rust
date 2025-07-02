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

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_kinesis::types::{Record, Shard};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::aggregated::deaggregate;

/**
 * This class contains data pertaining to a Lease. Distributed systems may use
 * leases to partition work across a fleet of workers. Each unit of work
 * (identified by a leaseKey) has a corresponding Lease. Every worker will
 * contend for all leases - only one worker will successfully take each one. The
 * worker should hold the lease until it is ready to stop processing the
 * corresponding unit of work, or until it fails. When the worker stops holding
 * the lease, another worker will take and hold the lease.
 *
 * The Java KCL uses DynamoDB to persist a Lease, which is duplicated here for
 * compatibility and consistency.
 */
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Lease {
    // if the memory overhead of all the duplicated strings becomes a problem,
    // we could consider using an Arc<String> or "internment". But since we'd
    // not expect to have much more than a few hundred leases in any given
    // instance, the overhead is so small as to not need to worry about it.
    /// shard_id of the shard associated to this lease. Primary key.
    pub lease_key: String,
    /// current owner (workerid) of the lease.
    #[serde(default)]
    pub lease_owner: Option<String>,
    /// incremented periodically by the worker as a heartbeat.
    #[serde(default)]
    pub lease_counter: i64,
    /// Most recently application-supplied checkpoint value. During fail over,
    /// the new worker will pick up after the old worker's last checkpoint.
    pub checkpoint: ExtendedSequenceNumber,

    /// A copy of the shard's parent id, so that non-leader nodes (who may
    /// have a stale view of ListShards) can build the full dependency graph.
    #[serde(default)]
    parent_shard_ids: Vec<String>,

    // the following are unused but written to be compatible with the Java KCL.
    // prefer the source of truth (the shards).
    #[serde(default)]
    owner_switches_since_checkpoint: i64,
    #[serde(default)]
    starting_hash_key: Option<String>,
    #[serde(default)]
    ending_hash_key: Option<String>,
    // the following are unused, and may be deleted in DDB for cleanliness
    // pub throughput_kbps: Option<f64>, // kilo bytes per second
    // pub checkpoint_owner: Option<String>,
    // pub pending_checkpoint: Option<ExtendedSequenceNumber>,
    // pub pending_checkpoint_state: Option<String>,
    // pub child_shard_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct KinesisClientRecord {
    pub record: aws_sdk_kinesis::types::Record,
    pub sub_sequence_number: i64,
    pub aggregated: bool,
}

impl KinesisClientRecord {
    pub fn extended_sequence_number(&self) -> ExtendedSequenceNumber {
        return ExtendedSequenceNumber {
            sequence_number: From::from(self.record.sequence_number.clone()),
            sub_sequence_number: self.sub_sequence_number,
        };
    }

    pub fn from(e: Record) -> Vec<KinesisClientRecord> {
        match deaggregate(e.data.as_ref()) {
            None => {
                return vec![KinesisClientRecord {
                    record: e,
                    sub_sequence_number: 0,
                    aggregated: false,
                }];
            }
            Some(wrapper) => wrapper
                .records
                .into_iter()
                .enumerate()
                .map(|(i, a)| KinesisClientRecord {
                    record: aws_sdk_kinesis::types::Record::builder()
                        .sequence_number(e.sequence_number.clone())
                        .set_approximate_arrival_timestamp(e.approximate_arrival_timestamp)
                        .data(From::from(a.data))
                        .partition_key(
                            wrapper.partition_key_table[a.partition_key_index as usize].clone(),
                        )
                        .set_encryption_type(e.encryption_type.clone())
                        .build()
                        .unwrap(),
                    sub_sequence_number: i as i64,
                    aggregated: true,
                })
                .collect_vec(),
        }
    }
}

/// Represents a two-part sequence number for records aggregated by the Kinesis
/// Producer Library.
///
/// The KPL combines multiple user records into a single Kinesis record. Each
/// user record therefore has an integer sub-sequence number, in addition to the
/// regular sequence number of the Kinesis record. The sub-sequence number is
/// used to checkpoint within an aggregated record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtendedSequenceNumber {
    pub sequence_number: SequenceNumber,
    #[serde(default)]
    pub sub_sequence_number: i64,
}

/// Corresponds to big integer shard sequence numbers from the Kinesis API or
/// one of four sentinel values used to indicate a special state in the lease
/// table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "String", from = "String")]
#[allow(non_camel_case_types)]
pub enum SequenceNumber {
    // sentinels
    AT_TIMESTAMP,
    TRIM_HORIZON,
    LATEST,
    // "0|([1-9]\d{0,128})"
    BIGINT(String),
    SHARD_END,
}

// ordering could follow the java convention for the sentinels AT_TIMESTAMP -3,
// TRIM_HORIZON = -2, LATEST = -1, <everything else is a biginteger>, SHARD_END
// = inf and considering sub_sequence_number.

impl ExtendedSequenceNumber {
    pub fn is_trim_horizon(&self) -> bool {
        return self.sequence_number == SequenceNumber::TRIM_HORIZON;
    }

    pub fn is_latest(&self) -> bool {
        return self.sequence_number == SequenceNumber::LATEST;
    }

    pub fn is_at_timestamp(&self) -> bool {
        return self.sequence_number == SequenceNumber::AT_TIMESTAMP;
    }

    pub fn is_ended(&self) -> bool {
        return self.sequence_number == SequenceNumber::SHARD_END;
    }
}

impl ToString for SequenceNumber {
    fn to_string(&self) -> String {
        match self {
            SequenceNumber::AT_TIMESTAMP => "AT_TIMESTAMP".to_string(),
            SequenceNumber::TRIM_HORIZON => "TRIM_HORIZON".to_string(),
            SequenceNumber::LATEST => "LATEST".to_string(),
            SequenceNumber::BIGINT(num) => num.clone(),
            SequenceNumber::SHARD_END => "SHARD_END".to_string(),
        }
    }
}

impl Into<String> for SequenceNumber {
    fn into(self) -> String {
        self.to_string()
    }
}

impl From<String> for SequenceNumber {
    fn from(s: String) -> SequenceNumber {
        match s.as_str() {
            "AT_TIMESTAMP" => SequenceNumber::AT_TIMESTAMP,
            "TRIM_HORIZON" => SequenceNumber::TRIM_HORIZON,
            "LATEST" => SequenceNumber::LATEST,
            "SHARD_END" => SequenceNumber::SHARD_END,
            _ => SequenceNumber::BIGINT(s), // could validate here and use TryFrom
        }
    }
}

pub(crate) fn shard_parent_ids(s: &Shard) -> Vec<&String> {
    s.parent_shard_id
        .iter()
        .chain(s.adjacent_parent_shard_id.iter())
        .collect_vec()
}

// ignores shard references that don't exist
pub(crate) fn shard_parents<'a>(shards: &'a HashMap<String, Shard>, s: &Shard) -> Vec<&'a Shard> {
    shard_parent_ids(s)
        .into_iter()
        .flat_map(|pid| shards.get(pid))
        .collect()
}

// not just the parents, but all of them
pub(crate) fn shard_ancestors<'a>(
    shards: &'a HashMap<String, Shard>,
    shard: &Shard,
) -> Vec<&'a Shard> {
    let mut parents = shard_parents(shards, shard);
    let higher = parents
        .iter()
        .flat_map(|p| shard_ancestors(shards, p))
        .collect_vec();
    parents.extend(higher);
    parents
}

impl Lease {
    pub fn from(s: &Shard, checkpoint: ExtendedSequenceNumber) -> Lease {
        Lease {
            lease_key: s.shard_id.clone(),
            lease_owner: Option::None,
            lease_counter: 0,
            owner_switches_since_checkpoint: 0,
            checkpoint,
            parent_shard_ids: shard_parent_ids(s).into_iter().cloned().collect_vec(),
            starting_hash_key: s
                .hash_key_range
                .as_ref()
                .map(|r| r.starting_hash_key.clone()),
            ending_hash_key: s.hash_key_range.as_ref().map(|r| r.ending_hash_key.clone()),
        }
    }

    /// is this a direct child of the given shard_ids?
    pub fn is_child_of(
        &self,
        candidates: &HashSet<&String>,
        leases: &HashMap<String, Lease>,
    ) -> bool {
        return self
            .parent_shard_ids(leases)
            .iter()
            .any(|p| candidates.contains(p));
    }

    pub fn parent_shard_ids<'a>(&self, leases: &'a HashMap<String, Lease>) -> Vec<&'a String> {
        leases
            .get(&self.lease_key)
            .map(|s| s.parent_shard_ids.iter().collect())
            .unwrap_or_default()
    }
}

// NOTE this is only useful for creating fresh attribute maps for a Lease, it is
// not useful for updating an existing entry since a Some that was changed to a
// None will not be updated by this in the underlying row. Constraints need to
// be added manually. It's also very verbose.
//
// It would be nice if this was simply auto-generated, given its limited use.
impl From<&Lease> for HashMap<String, AttributeValue> {
    fn from(lease: &Lease) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::new();

        item.insert(
            "leaseKey".to_string(),
            AttributeValue::S(lease.lease_key.to_string()),
        );

        if let Some(lease_owner) = &lease.lease_owner {
            item.insert(
                "leaseOwner".to_string(),
                AttributeValue::S(lease_owner.to_string()),
            );
        }

        item.insert(
            "leaseCounter".to_string(),
            AttributeValue::N(lease.lease_counter.to_string()),
        );

        item.insert(
            "ownerSwitchesSinceCheckpoint".to_string(),
            AttributeValue::N(lease.owner_switches_since_checkpoint.to_string()),
        );

        item.insert(
            "checkpoint".to_string(),
            AttributeValue::S(lease.checkpoint.sequence_number.to_string()),
        );

        item.insert(
            "checkpointSubSequenceNumber".to_string(),
            AttributeValue::N(lease.checkpoint.sub_sequence_number.to_string()),
        );

        if !lease.parent_shard_ids.is_empty() {
            item.insert(
                "parentShardId".to_string(),
                AttributeValue::Ss(lease.parent_shard_ids.clone()),
            );
        }

        if let Some(starting_hash_key) = &lease.starting_hash_key {
            item.insert(
                "startingHashKey".to_string(),
                AttributeValue::S(starting_hash_key.to_string()),
            );
        }

        if let Some(ending_hash_key) = &lease.ending_hash_key {
            item.insert(
                "endingHashKey".to_string(),
                AttributeValue::S(ending_hash_key.to_string()),
            );
        }

        item
    }
}

// It would be nice if this was auto generated
impl<'a> TryFrom<&'a HashMap<String, AttributeValue>> for Lease {
    // we could return the list of all fields that are missing (applicative
    // validation) but rust makes it so easy to return the first error
    // (monadic) so we do the idiomatic thing.
    type Error = (String, Option<&'a AttributeValue>);
    fn try_from(record: &'a HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        Ok(Lease {
            lease_key: safe_get_s(record, "leaseKey")?.clone(),
            lease_owner: safe_get_optional_s(record, "leaseOwner")?.cloned(),
            lease_counter: safe_get_n(record, "leaseCounter")?,
            owner_switches_since_checkpoint: safe_get_n(record, "ownerSwitchesSinceCheckpoint")?,
            checkpoint: ExtendedSequenceNumber {
                sequence_number: From::from(safe_get_s(record, "checkpoint")?.clone()),
                sub_sequence_number: safe_get_n(record, "checkpointSubSequenceNumber")?,
            },
            parent_shard_ids: safe_get_optional_ss(record, "parentShardId")?
                .cloned()
                .unwrap_or(Vec::new()),
            starting_hash_key: safe_get_optional_s(record, "startingHashKey")?.cloned(),
            ending_hash_key: safe_get_optional_s(record, "endingHashKey")?.cloned(),
        })
    }
}

// intended for extracting fields from a ddb record. the error duplicates the
// input key for convenience.
//
// we have variants for all the different kinds of types because it's not as
// easy as you'd think to compose them together from lambdas without a very
// abstract primitive.
//
// it feels like this kind of thing that should have a deserialisation library
// around it, but we seem to be the first to be needing it.
fn safe_get_s<'a>(
    record: &'a HashMap<String, AttributeValue>,
    key: &str,
) -> Result<&'a String, (String, Option<&'a AttributeValue>)> {
    record
        .get(key)
        .map(|v| v.as_s())
        .transpose()
        .map_err(Option::Some)
        .and_then(|o| o.ok_or(Option::None))
        .map_err(|e| (key.to_string(), e))
}

// the error never has a None, but it is kept like this for symmetry
fn safe_get_optional_s<'a>(
    record: &'a HashMap<String, AttributeValue>,
    key: &str,
) -> Result<Option<&'a String>, (String, Option<&'a AttributeValue>)> {
    match safe_get_s(record, key) {
        Ok(t) => Ok(Some(t)),
        Err((_, None)) => Ok(None),
        Err(e) => Err(e),
    }
}

// the Vec is never empty, because ddb doesn't allow empty sets, so
// None can be mapped into an empty list if the domain model allows.
fn safe_get_optional_ss<'a>(
    record: &'a HashMap<String, AttributeValue>,
    key: &str,
) -> Result<Option<&'a Vec<String>>, (String, Option<&'a AttributeValue>)> {
    record
        .get(key)
        .map(|v| v.as_ss())
        .transpose()
        .map_err(|e| (key.to_string(), Option::Some(e)))
}

fn safe_get_n<'a, F: FromStr>(
    record: &'a HashMap<String, AttributeValue>,
    key: &str,
) -> Result<F, (String, Option<&'a AttributeValue>)> {
    record
        .get(key)
        .map(|v| v.as_n().and_then(|n| n.parse::<F>().map_err(|_| v)))
        .transpose()
        .map_err(Option::Some)
        .and_then(|o| o.ok_or(Option::None))
        .map_err(|e| (key.to_string(), e))
}

#[allow(dead_code)]
fn safe_get_optional_n<'a, F: FromStr>(
    record: &'a HashMap<String, AttributeValue>,
    key: &str,
) -> Result<Option<F>, (String, Option<&'a AttributeValue>)> {
    match safe_get_n::<F>(record, key) {
        Ok(t) => Ok(Some(t)),
        Err((_, None)) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
pub mod dupes {
    use serde::{Deserialize, Serialize};

    // we can't use the https://serde.rs/remote-derive.html approach because it
    // doesn't support nested Option
    // https://github.com/serde-rs/serde/issues/723 and even though there is
    // unstable support for serde in the aws sdk, it is extremely complicated to
    // actually enable and use (plus isn't even guaranteed to agree with what
    // they are using internally).
    // https://smithy-lang.github.io/smithy-rs/design/rfcs/rfc0030_serialization_and_deserialization.html
    //
    // so we just copy the whole structure and implement Froms.
    #[derive(Serialize, Deserialize)]
    pub struct Shard {
        pub shard_id: String,
        parent_shard_id: Option<String>,
        adjacent_parent_shard_id: Option<String>,
        hash_key_range: Option<HashKeyRange>,
        sequence_number_range: Option<SequenceNumberRange>,
    }

    #[derive(Serialize, Deserialize)]
    struct HashKeyRange {
        starting_hash_key: String,
        ending_hash_key: String,
    }

    #[derive(Serialize, Deserialize)]
    struct SequenceNumberRange {
        starting_sequence_number: String,
        ending_sequence_number: Option<String>,
    }

    impl From<&Shard> for aws_sdk_kinesis::types::Shard {
        fn from(s: &Shard) -> aws_sdk_kinesis::types::Shard {
            aws_sdk_kinesis::types::Shard::builder()
                .shard_id(s.shard_id.clone())
                .set_parent_shard_id(s.parent_shard_id.clone())
                .set_adjacent_parent_shard_id(s.adjacent_parent_shard_id.clone())
                .set_hash_key_range(s.hash_key_range.as_ref().map(From::from))
                .set_sequence_number_range(s.sequence_number_range.as_ref().map(From::from))
                .build()
                .unwrap() // succeeds
        }
    }

    impl From<&aws_sdk_kinesis::types::Shard> for Shard {
        fn from(s: &aws_sdk_kinesis::types::Shard) -> Self {
            Self {
                shard_id: s.shard_id.clone(),
                parent_shard_id: s.parent_shard_id.clone(),
                adjacent_parent_shard_id: s.adjacent_parent_shard_id.clone(),
                hash_key_range: s.hash_key_range.as_ref().map(From::from),
                sequence_number_range: s.sequence_number_range.as_ref().map(From::from),
            }
        }
    }

    impl From<&HashKeyRange> for aws_sdk_kinesis::types::HashKeyRange {
        fn from(v: &HashKeyRange) -> aws_sdk_kinesis::types::HashKeyRange {
            aws_sdk_kinesis::types::HashKeyRange::builder()
                .starting_hash_key(v.starting_hash_key.clone())
                .ending_hash_key(v.ending_hash_key.clone())
                .build()
                .unwrap() // succeeds
        }
    }
    impl From<&aws_sdk_kinesis::types::HashKeyRange> for HashKeyRange {
        fn from(v: &aws_sdk_kinesis::types::HashKeyRange) -> Self {
            HashKeyRange {
                starting_hash_key: v.starting_hash_key.clone(),
                ending_hash_key: v.ending_hash_key.clone(),
            }
        }
    }

    impl From<&SequenceNumberRange> for aws_sdk_kinesis::types::SequenceNumberRange {
        fn from(v: &SequenceNumberRange) -> aws_sdk_kinesis::types::SequenceNumberRange {
            aws_sdk_kinesis::types::SequenceNumberRange::builder()
                .starting_sequence_number(v.starting_sequence_number.clone())
                .set_ending_sequence_number(v.ending_sequence_number.clone())
                .build()
                .unwrap() // succeeds
        }
    }

    impl From<&aws_sdk_kinesis::types::SequenceNumberRange> for SequenceNumberRange {
        fn from(v: &aws_sdk_kinesis::types::SequenceNumberRange) -> Self {
            SequenceNumberRange {
                starting_sequence_number: v.starting_sequence_number.clone(),
                ending_sequence_number: v.ending_sequence_number.clone(),
            }
        }
    }
}
