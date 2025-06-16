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

use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use aws_sdk_dynamodb::types::TableStatus;
use aws_sdk_dynamodb::Client;
use derive_more::{Display, Error};
use std::error::Error as StdError;
use std::time::Duration;
use tokio::time::{sleep, Instant};

/// Custom error type for DynamoDB waiter errors, simplifying the AWS SDK
/// errors into a single error type. AwsSdkError is used to wrap any other
/// errors that are not specifically handled.
#[derive(Debug, Display, Error)]
pub enum DynamoDbWaitError {
    #[display("Table '{}' does not exist.", _0)]
    #[error(ignore)]
    TableNotFound(String),

    #[display("Timeout while waiting for table '{}' to become active.", _0)]
    #[error(ignore)]
    Timeout(String),

    #[display("AWS SDK error: {}", _0)]
    AwsSdkError(#[error(source)] Box<dyn StdError + Send + Sync>),
}

impl From<SdkError<DescribeTableError>> for DynamoDbWaitError {
    fn from(err: SdkError<DescribeTableError>) -> Self {
        DynamoDbWaitError::AwsSdkError(Box::new(err))
    }
}

#[async_trait::async_trait]
pub trait DynamoDbWaiter {
    async fn wait_until_table_exists(
        &self,
        table_name: &str,
        max_wait: Duration,
    ) -> Result<(), DynamoDbWaitError>;
}

#[async_trait::async_trait]
impl DynamoDbWaiter for Client {
    async fn wait_until_table_exists(
        &self,
        table_name: &str,
        max_wait: Duration,
    ) -> Result<(), DynamoDbWaitError> {
        let start = Instant::now();
        let poll_interval = Duration::from_secs(1);

        loop {
            match self.describe_table().table_name(table_name).send().await {
                Ok(response) => {
                    if let Some(table) = response.table {
                        if let Some(status) = table.table_status {
                            if status == TableStatus::Active {
                                tracing::debug!("Table '{}' is active.", table_name);
                                return Ok(());
                            } else {
                                tracing::debug!(
                                    "Table '{}' is not active yet. Status: {:?}",
                                    table_name,
                                    status
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    // If the error is a ResourceNotFoundException, then the table does not exist,
                    // and the user should create it beforehand to avoid us wastefully waiting here
                    if let SdkError::ServiceError(service_err) = &err {
                        if service_err.err().is_resource_not_found_exception() {
                            return Err(DynamoDbWaitError::TableNotFound(table_name.to_string()));
                        } else {
                            return Err(DynamoDbWaitError::from(err));
                        }
                    } else {
                        return Err(DynamoDbWaitError::from(err));
                    }
                }
            }

            if start.elapsed() >= max_wait {
                return Err(DynamoDbWaitError::Timeout(table_name.to_string()));
            }

            sleep(poll_interval).await;
        }
    }
}
