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

//! Configuration examples showing different deployment scenarios.
//!
//! This module demonstrates various configuration patterns for different
//! use cases and deployment scenarios.

use disney_kinesis_client::*;

/// Configuration for a high-throughput, low-latency consumer
pub fn high_throughput_config() -> (ManagementConfig, LeaseConfig) {
    let management_config = ManagementConfig {
        leases_to_acquire: 5,  // Aggressive lease acquisition
        max_leases: 50,        // High limit for powerful instances
        start_strategy: StartStrategy::Latest, // Don't replay old data
    };

    let lease_config = LeaseConfig::PollingConfig {
        limit: 10000, // Maximum batch size for high throughput
    };

    (management_config, lease_config)
}

/// Configuration for a batch processing consumer that can handle delays
pub fn batch_processing_config() -> (ManagementConfig, LeaseConfig) {
    let management_config = ManagementConfig {
        leases_to_acquire: 2,  // Conservative acquisition
        max_leases: 20,        // Moderate limit
        start_strategy: StartStrategy::Oldest, // Process all data
    };

    let lease_config = LeaseConfig::PollingConfig {
        limit: 5000, // Moderate batch size
    };

    (management_config, lease_config)
}

/// Configuration for development/testing with small streams
pub fn development_config() -> (ManagementConfig, LeaseConfig) {
    let management_config = ManagementConfig {
        leases_to_acquire: 1,  // One at a time for testing
        max_leases: 5,         // Small limit
        start_strategy: StartStrategy::Latest,
    };

    let lease_config = LeaseConfig::PollingConfig {
        limit: 100, // Small batches for easier debugging
    };

    (management_config, lease_config)
}

/// Configuration for cost-sensitive deployments
pub fn cost_optimized_config() -> (ManagementConfig, LeaseConfig) {
    let management_config = ManagementConfig {
        leases_to_acquire: 1,  // Minimal lease acquisition
        max_leases: 10,        // Limit concurrent processing
        start_strategy: StartStrategy::Latest, // Don't replay expensive data
    };

    let lease_config = LeaseConfig::PollingConfig {
        limit: 1000, // Balanced batch size
    };

    (management_config, lease_config)
}

/// Configuration for replay/catchup scenarios
pub fn replay_config(start_timestamp: i64) -> (ManagementConfig, LeaseConfig) {
    let management_config = ManagementConfig {
        leases_to_acquire: 10, // Aggressive for faster catchup
        max_leases: 100,       // High limit for replay scenarios
        start_strategy: StartStrategy::Timestamp(start_timestamp),
    };

    let lease_config = LeaseConfig::PollingConfig {
        limit: 10000, // Maximum throughput for catchup
    };

    (management_config, lease_config)
}

/// Example of environment-based configuration
pub fn from_environment() -> Result<(ManagementConfig, LeaseConfig), Box<dyn std::error::Error>> {
    use std::env;

    let leases_to_acquire = env::var("KINESIS_LEASES_TO_ACQUIRE")
        .unwrap_or_else(|_| "2".to_string())
        .parse::<usize>()?;

    let max_leases = env::var("KINESIS_MAX_LEASES")
        .unwrap_or_else(|_| "20".to_string())
        .parse::<usize>()?;

    let start_strategy = match env::var("KINESIS_START_STRATEGY").as_deref() {
        Ok("OLDEST") => StartStrategy::Oldest,
        Ok("LATEST") => StartStrategy::Latest,
        Ok(timestamp_str) if timestamp_str.starts_with("TIMESTAMP:") => {
            let timestamp = timestamp_str[10..].parse::<i64>()?;
            StartStrategy::Timestamp(timestamp)
        }
        _ => StartStrategy::Latest, // Default
    };

    let limit = env::var("KINESIS_BATCH_LIMIT")
        .unwrap_or_else(|_| "1000".to_string())
        .parse::<i32>()?;

    let management_config = ManagementConfig {
        leases_to_acquire,
        max_leases,
        start_strategy,
    };

    let lease_config = LeaseConfig::PollingConfig { limit };

    Ok((management_config, lease_config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_high_throughput_config() {
        let (mgmt, lease) = high_throughput_config();
        assert_eq!(mgmt.leases_to_acquire, 5);
        assert_eq!(mgmt.max_leases, 50);
        assert_eq!(mgmt.start_strategy, StartStrategy::Latest);
        
        if let LeaseConfig::PollingConfig { limit } = lease {
            assert_eq!(limit, 10000);
        } else {
            panic!("Expected PollingConfig");
        }
    }

    #[test]
    fn test_development_config() {
        let (mgmt, lease) = development_config();
        assert_eq!(mgmt.leases_to_acquire, 1);
        assert_eq!(mgmt.max_leases, 5);
        
        if let LeaseConfig::PollingConfig { limit } = lease {
            assert_eq!(limit, 100);
        } else {
            panic!("Expected PollingConfig");
        }
    }

    #[test]
    fn test_replay_config() {
        let timestamp = 1640995200000; // Jan 1, 2022
        let (mgmt, lease) = replay_config(timestamp);
        
        assert_eq!(mgmt.leases_to_acquire, 10);
        assert_eq!(mgmt.start_strategy, StartStrategy::Timestamp(timestamp));
    }

    #[test]
    fn test_environment_config_defaults() {
        // Clear any existing environment variables for this test
        std::env::remove_var("KINESIS_LEASES_TO_ACQUIRE");
        std::env::remove_var("KINESIS_MAX_LEASES");
        std::env::remove_var("KINESIS_START_STRATEGY");
        std::env::remove_var("KINESIS_BATCH_LIMIT");

        let result = from_environment().unwrap();
        let (mgmt, lease) = result;
        
        assert_eq!(mgmt.leases_to_acquire, 2);
        assert_eq!(mgmt.max_leases, 20);
        assert_eq!(mgmt.start_strategy, StartStrategy::Latest);
        
        if let LeaseConfig::PollingConfig { limit } = lease {
            assert_eq!(limit, 1000);
        } else {
            panic!("Expected PollingConfig");
        }
    }

    #[test]
    fn test_environment_config_custom() {
        std::env::set_var("KINESIS_LEASES_TO_ACQUIRE", "5");
        std::env::set_var("KINESIS_MAX_LEASES", "30");
        std::env::set_var("KINESIS_START_STRATEGY", "OLDEST");
        std::env::set_var("KINESIS_BATCH_LIMIT", "2000");

        let result = from_environment().unwrap();
        let (mgmt, lease) = result;
        
        assert_eq!(mgmt.leases_to_acquire, 5);
        assert_eq!(mgmt.max_leases, 30);
        assert_eq!(mgmt.start_strategy, StartStrategy::Oldest);
        
        if let LeaseConfig::PollingConfig { limit } = lease {
            assert_eq!(limit, 2000);
        } else {
            panic!("Expected PollingConfig");
        }

        // Clean up
        std::env::remove_var("KINESIS_LEASES_TO_ACQUIRE");
        std::env::remove_var("KINESIS_MAX_LEASES");
        std::env::remove_var("KINESIS_START_STRATEGY");
        std::env::remove_var("KINESIS_BATCH_LIMIT");
    }
}

#[cfg(not(test))]
fn main() {
    println!("This is a configuration examples module. Use the functions in your application:");
    println!("  - high_throughput_config()");
    println!("  - batch_processing_config()");
    println!("  - development_config()");
    println!("  - cost_optimized_config()");
    println!("  - replay_config(timestamp)");
    println!("  - from_environment()");
}
