# Project Meridian - Testing and Validation Results

## 1. Overview

This document summarizes the functional, reliability, storage, and performance validation performed on Project Meridian.

Tests were executed against the Kafka, Spark Structured Streaming, and Apache Iceberg development environment.

## 2. Test Summary

| Test Area | Validation | Result |
|---|---|---|
| Kafka connectivity | Spark container connectivity to Kafka through TCP proxy | Verified |
| Event ingestion | Kafka events consumed by Spark Structured Streaming | Verified |
| Data quality | Valid and invalid events routed to separate tables | Verified |
| Idempotency | Kafka partition and offset based duplicate handling | Verified |
| Recovery | Graceful restart using existing checkpoints | Verified |
| End-to-end pipeline | Bronze, Quarantine, and Gold table validation | Verified |
| Partition evolution | Monthly to daily partition evolution | Verified |
| Query optimization | Iceberg filter pushdown and column pruning | Verified |
| File compaction | Small-file rewrite on a dedicated test table | Verified |

## 3. Data Quality Validation

Observed results during end-to-end testing:

| Metric | Records |
|---|---:|
| Accepted Bronze records | 17 |
| Quarantined records | 6 |
| Total accounted records | 23 |

### Quarantine breakdown

| Validation reason | Records |
|---|---:|
| INVALID_JSON | 3 |
| MISSING_SHIPMENT_ID | 3 |

The accepted and quarantined counts account for the records observed in the destination tables during testing. They are test observations, not production throughput or quality benchmarks.

## 4. Idempotency Validation

The dedicated Scala idempotency producer published two events with the same shipment ID.

| Test | Kafka Partition | Kafka Offset |
|---|---:|---:|
| First event | 0 | 17 |
| Second event | 0 | 18 |

Both records were retained because they had different Kafka offsets.

The application uses Kafka partition and offset metadata as the source-record identity. This prevents the same Kafka record from being inserted repeatedly during replay or reprocessing.

Business-identical events with different offsets are not treated as duplicates.

## 5. Streaming Recovery Validation

The streaming application was gracefully stopped and restarted using its existing checkpoint locations.

Validation included:

- Streaming process inspection.
- Checkpoint directory inspection.
- Offset and commit file inspection.
- Bronze record count comparison before and after restart.

The Bronze record count remained unchanged at 17 after the graceful restart.

This validates checkpoint-based graceful recovery. A hard-crash failure-injection test was not performed.

## 6. Iceberg Partition Evolution

A dedicated test table was initially partitioned by month using the event timestamp.

The partition specification was then evolved to include day-level partitioning.

Validation confirmed:

- Existing files retained their original monthly partition specification.
- New files used the evolved daily partition specification.
- Existing data remained readable after the evolution.

## 7. Query Performance Validation

`EXPLAIN FORMATTED` was used to inspect an Iceberg query execution plan.

The plan showed:

- Iceberg filter pushdown.
- Column pruning.
- No Exchange operation for the tested simple query.

File metadata and partition values were inspected to understand the table layout.

A formal benchmark of scan bytes, execution time, or throughput was not performed.

## 8. Small-File Compaction Validation

A dedicated Iceberg test table was populated with six small data files.

Before compaction:

| Metric | Value |
|---|---:|
| Data files | 6 |
| Records | 6 |
| Total file size | 9,549 bytes |

After running Iceberg `rewrite_data_files`:

| Metric | Value |
|---|---:|
| Data files | 1 |
| Records | 6 |
| Total file size | 1,804 bytes |

The compacted table retained all six records.

Snapshot expiration was also tested on this dedicated test table, and the records remained readable afterward.

## 9. End-to-End Pipeline Validation

The following components were inspected during end-to-end validation:

1. Kafka broker and shipment topic.
2. Kafka topic partitions and offsets.
3. Spark-to-Kafka connectivity.
4. Spark Structured Streaming process.
5. Streaming checkpoint offsets.
6. Bronze Iceberg records.
7. Quarantine Iceberg records.
8. Gold window analytics records.

The observed destination-table counts were 17 Bronze records and 6 Quarantine records.

## 10. Monitoring Evidence

Monitoring checks included:

- Streaming process status.
- Application log inspection.
- Checkpoint offset and commit files.
- Kafka partition end offsets.
- Iceberg snapshots and data-file metadata.
- Data quality counts and rejection reasons.

No matching ERROR, Exception, FAILED, or FATAL entries were found in the inspected streaming log output.

## 11. Known Limitations and Future Improvements

- Kafka development configuration currently uses replication factor 1.
- Small-file accumulation should be managed with scheduled or threshold-based compaction.
- Business-level deduplication requires a stable event identifier or idempotency key.
- Production monitoring should include automated alerts for rejection rates and streaming lag.
- Hard-crash recovery and sustained-load performance benchmarking remain future validation areas.
- Production deployment should use multiple Kafka brokers and an appropriate replication strategy.

## 12. Conclusion

Project Meridian was validated across event ingestion, stream processing, data quality routing, checkpoint-based graceful recovery, Kafka-offset idempotency, Iceberg storage, partition evolution, query-plan inspection, and file compaction.

The results demonstrate the core functionality of the development platform while identifying areas for further production hardening.
