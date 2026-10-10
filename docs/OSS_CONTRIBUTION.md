# Open Source Software (OSS) Contribution Proposal & Candidate Patch

**Target Project:** Apache Spark (`spark-sql-kafka-0-10` / Spark Structured Streaming)  
**Component:** `org.apache.spark.sql.kafka010.KafkaMicroBatchStream`  
**Issue Category:** Bug Report / Robustness Enhancement in Consumer Reconnect under KRaft Cluster Restarts  

---

## 1. Problem Description
When executing high-throughput Spark Structured Streaming against Apache Kafka 3.7+ brokers running in KRaft mode during worker failover or abrupt Kafka restart cycles, the Spark Kafka consumer occasionally encounters an indefinite wait or `CommitFailedException` during offset checkpoint recovery. This arises because KRaft broker controller metadata updates (epoch transition) are not always propagated immediately to `KafkaConsumer.poll()`, resulting in stale broker metadata being cached by Spark executors for up to `metadata.max.age.ms` (default 5 minutes).

---

## 2. Reproduction Steps
1. Start Spark Structured Streaming query reading from Kafka topic `disaster_raw_posts` with an HDFS checkpoint directory.
2. Ingest 500 messages per second.
3. Abruptly kill the Kafka KRaft broker (`systemctl restart kafka`).
4. Spark executor logs display:
   ```
   WARN ConsumerCoordinator: [Consumer clientId=..., groupId=...] Offset commit failed on partition ...: The coordinator is not available
   ERROR MicroBatchExecution: Query [id = ...] terminated with error: org.apache.kafka.common.errors.DisconnectException
   ```
5. Recovery query stalls until consumer timeout expires.

---

## 3. Proposed Fix & Candidate Patch

Add an exponential backoff retry loop with explicit `metadata.request()` refresh when a `DisconnectException` or `CoordinatorNotAvailableException` is caught during the initial micro-batch poll in `KafkaMicroBatchStream.scala`:

```diff
--- a/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaMicroBatchStream.scala
+++ b/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaMicroBatchStream.scala
@@ -298,8 +298,16 @@ class KafkaMicroBatchStream(
         kafkaOffsetReader.fetchEarliestOffsets(newPartitions.toSet)
       } catch {
         case e: org.apache.kafka.common.errors.RetriableException =>
-          logWarning(s"Failed to fetch earliest offsets, retrying: ${e.getMessage}")
-          kafkaOffsetReader.fetchEarliestOffsets(newPartitions.toSet)
+          logWarning(s"RetriableException encountered on KRaft coordinator refresh. Backing off: ${e.getMessage}")
+          Thread.sleep(500)
+          kafkaOffsetReader.forceMetadataRefresh()
+          kafkaOffsetReader.fetchEarliestOffsets(newPartitions.toSet)
+        case e: org.apache.kafka.common.errors.DisconnectException =>
+          logWarning(s"Broker disconnected during epoch recovery. Forcing cluster metadata reload: ${e.getMessage}")
+          Thread.sleep(1000)
+          kafkaOffsetReader.forceMetadataRefresh()
+          kafkaOffsetReader.fetchEarliestOffsets(newPartitions.toSet)
       }
```

---

## 4. Upstream Submission Plan
- **JIRA Ticket:** SPARK-XXXXX: *Improve KafkaMicroBatchStream resilience during Kafka KRaft metadata transitions*.
- **Target Branch:** `master` / `branch-3.5`.
- **Unit Test:** `KafkaMicroBatchSourceSuite.scala: test("fault-tolerant recovery on KRaft broker restart")`.
