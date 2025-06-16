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

package test

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Path }
import java.nio.file.StandardOpenOption.{ CREATE, TRUNCATE_EXISTING }
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters.CollectionHasAsScala

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended, KinesisClientUtil }
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.events.{ InitializationInput, LeaseLostInput, ProcessRecordsInput, ShardEndedInput, ShutdownRequestedInput }
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.{ ShardRecordProcessor, ShardRecordProcessorFactory, SingleStreamTracker }
import software.amazon.kinesis.retrieval.polling.PollingConfig

object Worker {
  def main(args: Array[String]): Unit = {
    require(args.length == 2)
    val stream_name = args(0)
    val table_name = args(1)
    require(stream_name.nonEmpty)
    require(table_name.nonEmpty)

    if (System.getProperty("org.slf4j.simpleLogger.defaultLogLevel") == null) {
      System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error")
    }

    val received = new ConcurrentLinkedQueue[String]()

    start(stream_name, table_name, 64, TRIM_HORIZON, worker(received))
    Thread.sleep(120_000) // the java kcl starts up super slow the first time

    var last_size = -1
    while (received.size() > last_size) {
      last_size = received.size()
      System.out.println(s"java-kcl-worker: seen $last_size records")
      Thread.sleep(20_000)
    }
    System.out.println(s"java-kcl-worker: finishing up")

    val summary = received.asScala.mkString("\n")
    Files.writeString(Path.of("java-kcl-worker.txt"), summary, CREATE, TRUNCATE_EXISTING): Unit

    sys.exit(0)
  }

  val TRIM_HORIZON = InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

  def worker(h: ConcurrentLinkedQueue[String]): ShardRecordProcessorFactory = new ShardRecordProcessorFactory {
    override def shardRecordProcessor(): ShardRecordProcessor = new ShardRecordProcessor {
      @volatile var shard_id: String = null
      override def initialize(x: InitializationInput): Unit = {
        shard_id = x.shardId()
        System.out.println(s"java-kcl-worker: starting $shard_id at ${x.extendedSequenceNumber}")
      }
      override def leaseLost(x: LeaseLostInput): Unit = {
        System.out.println(s"java-kcl-worker: lost lease on $shard_id")
      }
      override def processRecords(x: ProcessRecordsInput): Unit = {
        // System.out.println(s"$shard_id seen ${x.records.size} records")
        x.records.asScala.foreach { r =>
          h.add(StandardCharsets.UTF_8.decode(r.data()).toString())
        }
        x.checkpointer().checkpoint()
      }
      override def shardEnded(x: ShardEndedInput): Unit = {
        System.out.println(s"java-kcl-worker: $shard_id ended")
        x.checkpointer().checkpoint()
      }
      override def shutdownRequested(x: ShutdownRequestedInput): Unit = { }
    }
  }

  // parameters roughly match the rust equivalents
  def start(
    stream_name: String,
    table_name: String,
    max_leases: Int,
    start_strategy: InitialPositionInStreamExtended,
    factory: ShardRecordProcessorFactory
  ): Thread = {
    // all default creds and regions for the clients...
    val kinesis_client = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder())
    val ddb_client = DynamoDbAsyncClient.builder().build()
    val cw_client = CloudWatchAsyncClient.builder().build()
    val identifier = UUID.randomUUID().toString()

    val configsBuilder = new ConfigsBuilder(
      new SingleStreamTracker(stream_name, start_strategy),
      table_name,
      kinesis_client,
      ddb_client,
      cw_client,
      identifier,
      factory
    )

    val scheduler = new Scheduler(
      configsBuilder.checkpointConfig(),
      configsBuilder.coordinatorConfig(),
      configsBuilder.leaseManagementConfig().maxLeasesForWorker(max_leases),
      configsBuilder.lifecycleConfig(),
      configsBuilder.metricsConfig().metricsLevel(MetricsLevel.SUMMARY),
      configsBuilder.processorConfig(),
      configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig(stream_name, kinesis_client).maxRecords(1000))
    )

    Runtime.getRuntime().addShutdownHook(
      new Thread {
        override def run(): Unit = scheduler.startGracefulShutdown(): Unit
      }
    )

    val t = new Thread(scheduler, s"kcl:$stream_name:$table_name")
    t.setDaemon(false)
    t.start()
    t
  }
}
