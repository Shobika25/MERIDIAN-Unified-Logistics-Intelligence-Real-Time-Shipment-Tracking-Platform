# Project Meridian - Deployment and Setup Guide

## 1. Overview

Project Meridian is a Scala-first logistics data engineering platform built using Apache Kafka, Apache Spark Structured Streaming, Apache Iceberg, and Azure Data Lake Storage Gen2 (ADLS Gen2).

This guide describes the development environment, dependencies, configuration, project structure, and deployment components.

## 2. Technology Requirements

| Component | Version or Configuration |
|---|---|
| Scala | 2.13.17 |
| Apache Spark | 4.1.3 |
| Apache Kafka | 3.9.1 |
| Apache Iceberg | 1.12.0 |
| Hadoop Azure | 3.4.2 |
| Docker runtime Java | 17 |
| GitHub Actions Java | 21 |
| Build tool | sbt 1.11.7 |
| Storage | Azure Data Lake Storage Gen2 |
| Runtime | Docker containers |

Java 17 is used by the configured Spark Docker image. GitHub Actions uses Java 21 for the CI compilation workflow.

## 3. Project Structure

```text
meridian-iceberg/
├── .github/
│   └── workflows/
│       └── scala-build.yml
├── Architecture/
│   ├── Star-Schema-Diagram.png
│   └── system-architecture.md
├── Data Ingestion/
│   └── kafka-ingestion.md
├── Database/
│   └── DDL_Execution.sql
├── Development/
│   └── spark-streaming-processing.md
├── Logging/
│   └── monitoring-and-observability.md
├── docs/
│   └── deployment-guide.md
├── src/
│   └── main/
│       ├── java/
│       │   └── KafkaTcpProxy.java
│       └── scala/
│           └── com/meridian/
│               ├── CreateInsertTestSource.scala
│               ├── CreateUpdateTestSource.scala
│               ├── FactShipmentIcebergBatch.scala
│               ├── IdempotencyTestProducer.scala
│               ├── KafkaConnectivityTest.scala
│               ├── ShipmentEventIcebergStreaming.scala
│               ├── ShipmentEventProducer.scala
│               └── ShipmentEventStreaming.scala
├── project/
│   └── build.properties
├── .gitignore
├── build.sbt
└── start-meridian-iceberg.sh
```

## 4. Build Configuration

The project uses sbt to manage Scala application dependencies.

The build configuration declares:

- Scala 2.13.17.
- Spark SQL 4.1.3.
- Spark SQL Kafka integration 4.1.3.
- Kafka Clients 3.9.1.

Spark dependencies use `Provided` scope because the Spark runtime supplies them during execution.

Iceberg and Hadoop Azure runtime packages are supplied through the Spark launcher.

## 5. Scala Application Components

The main Scala source package is:

`src/main/scala/com/meridian/`

| Source file | Responsibility |
|---|---|
| ShipmentEventProducer.scala | Shipment event production |
| IdempotencyTestProducer.scala | Controlled idempotency testing |
| KafkaConnectivityTest.scala | Kafka connectivity testing |
| ShipmentEventIcebergStreaming.scala | Iceberg streaming application |
| ShipmentEventStreaming.scala | Additional streaming implementation |
| FactShipmentIcebergBatch.scala | Batch processing |
| CreateInsertTestSource.scala | Insert test data |
| CreateUpdateTestSource.scala | Update test data |

The Java TCP proxy is located at:

`src/main/java/KafkaTcpProxy.java`

## 6. Docker Runtime

The development environment uses two containers.

| Container | Image | Purpose |
|---|---|---|
| meridian-iceberg | apache/spark:4.1.3-scala2.13-java17-python3-ubuntu | Spark and Iceberg execution |
| social-media-kafka | apache/kafka:3.9.1 | Kafka broker |

Exposed development ports:

- Kafka broker: 9092
- Spark UI: 4040

The Spark container connects to Kafka through the configured TCP proxy.

## 7. Azure Data Lake Storage Configuration

The Iceberg warehouse is stored in Azure Data Lake Storage Gen2.

Warehouse location:

`abfss://meridian@adls2501.dfs.core.windows.net/iceberg/warehouse`

The launcher `start-meridian-iceberg.sh`:

1. Creates Hadoop configuration and Ivy directories.
2. Reads the ADLS SAS credential from a Docker secret file or the `AZURE_SAS_TOKEN` environment variable.
3. Generates the Hadoop `core-site.xml` configuration.
4. Restricts access to the generated configuration file.
5. Sets `HADOOP_CONF_DIR`.
6. Starts Spark SQL with Iceberg and Hadoop Azure runtime packages.
7. Configures the local and ADLS Iceberg catalogs.

Never commit SAS tokens, secret files, or generated credential configuration to source control.

## 8. Iceberg Runtime Configuration

The launcher supplies these runtime packages:

- `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0`
- `org.apache.hadoop:hadoop-azure:3.4.2`

The ADLS catalog is configured as a Hadoop catalog using the Azure warehouse location.

The launcher also enables the Iceberg Spark SQL extensions.

## 9. Development Build

From the project root, compile the Scala and Java project using:

```bash
sbt clean compile
```

To create the standard project package:

```bash
sbt package
```

The resulting package is generated under the sbt target directory.

The Spark runtime must provide the dependencies declared with `Provided` scope.

## 10. CI Build

The GitHub Actions workflow is located at:

`.github/workflows/scala-build.yml`

It:

- Checks out the repository.
- Sets up Java 21.
- Sets up sbt.
- Runs `sbt clean compile`.

This workflow currently validates compilation. It does not execute Kafka, Spark streaming, or ADLS integration tests.

## 11. Deployment Considerations

Before deploying to production:

- Configure multiple Kafka brokers and an appropriate replication factor.
- Store Azure credentials in a secure secret manager.
- Use durable storage for streaming checkpoints.
- Configure suitable Spark resource allocations.
- Monitor Kafka offsets and streaming progress.
- Configure data quality alerts.
- Schedule Iceberg maintenance and compaction.
- Establish log retention and centralized monitoring.
- Validate recovery procedures before production release.

## 12. Security Notes

The repository must not contain:

- Azure SAS tokens.
- Docker secret files.
- Private keys or certificates.
- Environment files containing credentials.
- Generated credential configuration files.

Use the project's secret-handling mechanism when configuring ADLS access.

## 13. Summary

Project Meridian combines Scala application development, Kafka event ingestion, Spark processing, Iceberg table management, and ADLS Gen2 storage.

The documented configuration reflects the development environment. Production deployment requires additional security, availability, monitoring, and capacity planning.