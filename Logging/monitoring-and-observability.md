# Project Meridian - Monitoring and Observability

## 1. Overview

Project Meridian uses application logs, Kafka offsets, Spark checkpoints, and Apache Iceberg metadata to inspect streaming health and data pipeline behavior.

## 2. Monitoring Areas

| Component | Monitoring Method |
|---|---|
| Kafka | Topic partitions and end offsets |
| Spark Structured Streaming | Process status and application logs |
| Checkpointing | Offset and commit files |
| Bronze Iceberg | Record counts, snapshots, and data files |
| Quarantine Iceberg | Rejected record counts and validation reasons |
| Gold Iceberg | Aggregated metrics and table metadata |

## 3. Application Logs

The streaming application writes runtime information to its application log.

Log inspection can help identify:

- Application startup and shutdown.
- Kafka connectivity issues.
- Streaming execution errors.
- Processing and storage failures.

During the recorded validation, the inspected log output contained no matching ERROR, Exception, FAILED, or FATAL entries.

## 4. Checkpoint Monitoring

The project uses persistent checkpoint directories for streaming progress.

Main event ingestion checkpoint:

`/tmp/meridian-checkpoints/shipment-events-iceberg`

Window analytics checkpoint:

`/tmp/meridian-checkpoints/shipment-event-window-analytics`

Checkpoint inspection includes:

- Offset files.
- Commit files.
- Source metadata.
- Latest completed batch information.

The checkpoint offsets can be compared with Kafka partition end offsets to understand the progress recorded by the streaming application.

## 5. Kafka Monitoring

The shipment topic is:

`meridian-shipment-events`

The development topic uses three partitions and replication factor one.

Monitoring includes:

- Topic availability.
- Partition leadership.
- Partition end offsets.
- In-sync replica status.
- Comparison with Spark checkpoint offsets.

The development configuration uses a single replica per partition and therefore does not provide broker-level replication redundancy.

## 6. Data Quality Monitoring

The pipeline separates accepted records from rejected records.

Observed validation results:

| Metric | Records |
|---|---:|
| Bronze accepted records | 17 |
| Quarantine rejected records | 6 |
| Total accounted records | 23 |

Observed rejection reasons:

| Reason | Records |
|---|---:|
| INVALID_JSON | 3 |
| MISSING_SHIPMENT_ID | 3 |

These are development test observations. Production monitoring should track rejection rates over time and alert when configured thresholds are exceeded.

## 7. Iceberg Metadata Monitoring

Iceberg metadata provides visibility into table changes and storage layout.

The project inspected:

- Snapshot history.
- Metadata log entries.
- Data file counts and sizes.
- Record counts.
- Partition information.
- Delete file metadata.

These details help identify small-file accumulation, verify table updates, and plan maintenance operations.

## 8. Storage Optimization

Low-volume streaming writes can produce small data files.

The project tested the Iceberg `rewrite_data_files` procedure on a dedicated test table.

Production deployments should consider scheduled or threshold-based compaction rather than allowing small files to accumulate indefinitely.

## 9. Production Monitoring Recommendations

Future production monitoring should include:

- Automated streaming failure alerts.
- Kafka consumer lag and processing delay monitoring.
- Data quality and quarantine-rate alerts.
- Checkpoint health monitoring.
- Iceberg snapshot and storage growth monitoring.
- Small-file detection and compaction scheduling.
- Dashboards for Bronze, Quarantine, and Gold metrics.
- Centralized log collection and retention.

## 10. Summary

Project Meridian uses Kafka offsets, Spark checkpoints, application logs, and Iceberg metadata as its primary development monitoring evidence.

Automated alerting, centralized observability, and sustained-load monitoring remain production-hardening improvements.
