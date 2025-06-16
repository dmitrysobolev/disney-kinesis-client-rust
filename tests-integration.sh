#!/bin/bash

# Runs all the integration tests as a batch job.
# The envvar TEST_KINESIS_STREAM may be set to
# change the default stream name.

set -e

# build
cargo test --no-run
cd tests/java
sbt assembly
cd -

# setup
cargo test populate_stream -- --ignored --nocapture

# rust tests
cargo test single_worker -- --ignored --nocapture
cargo test multi_workers -- --ignored --nocapture

# multilang tests
cargo test multilang_workers -- --ignored --nocapture
