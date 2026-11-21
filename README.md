# Project Meridian - Iceberg Batch Processing

Scala-based incremental batch processing for a logistics intelligence platform.

## Technology Stack
- Apache Spark 4.1.3
- Scala 2.13.17
- Apache Iceberg 1.12.0
- Hadoop Azure 3.4.2
- Java 17
- Azure Data Lake Storage Gen2

## Architecture
ADLS Parquet Source -> Scala Spark Batch -> Data Quality Validation -> Change Detection -> Iceberg MERGE -> Gold Fact Table

## Scala Applications
- FactShipmentIcebergBatch.scala
- CreateUpdateTestSource.scala
- CreateInsertTestSource.scala

## Implemented Features
- Parquet source ingestion
- Null and duplicate shipment ID validation
- 22-column schema validation
- Incremental INSERT and UPDATE using Iceberg MERGE
- Null-safe change detection
- Idempotent batch execution
- Iceberg snapshot reporting
- Post-processing validation

## Validation Results
| Metric | Result |
|---|---:|
| Initial target rows | 116 |
| Final target rows | 117 |
| Distinct shipment IDs | 117 |
| Null shipment IDs | 0 |
| Update test | Passed |
| Insert test | Passed |
| Idempotency test | Passed |

Tests used the separate table: meridian_adls.gold.fact_shipment_batch_test.

The production table was not modified by these tests.

## Build

```bash
sbt clean compile
sbt package
```

## Security
- Never commit SAS tokens or Azure credentials.
- Use environment variables for secrets.
- Exclude `.secrets/`, generated JARs, and warehouse data.

## Future Work
- Kafka integration
- Scala Structured Streaming
- GitHub Actions CI/CD
- Kubernetes deployment
