package com.meridian

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object ShipmentEventIcebergStreaming {

  private val topic = "meridian-shipment-events"

  private val acceptedTable =
    "meridian_adls.bronze.shipment_events"

  private val quarantineTable =
    "meridian_adls.bronze.shipment_events_quarantine"

  private val checkpointLocation =
    "/tmp/meridian-checkpoints/shipment-events-iceberg"

  private val analyticsCheckpointLocation =
    "/tmp/meridian-checkpoints/shipment-event-window-analytics"

  private val shipmentSchema = StructType(Seq(
    StructField("shipment_id", StringType, true),
    StructField("event_type", StringType, true),
    StructField("location", StringType, true),
    StructField("event_time", StringType, true),
    StructField("_corrupt_record", StringType, true)
  ))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Meridian-Shipment-Event-Iceberg-Streaming")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.meridian_adls",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.meridian_adls.type", "hadoop")
      .config(
        "spark.sql.catalog.meridian_adls.warehouse",
        "abfss://meridian@adls2501.dfs.core.windows.net/iceberg/warehouse"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {

      println("========== MERIDIAN ICEBERG STREAMING ==========")

      // STEP 1: Read new Kafka events

      val kafkaEvents = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()

      // STEP 2: Preserve raw JSON and Kafka metadata

      val rawEvents = kafkaEvents.select(
        col("value").cast("string").alias("event_json"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
      )

      // STEP 3: Parse JSON

      val parsedEvents = rawEvents
        .withColumn(
          "shipment",
          from_json(
            col("event_json"),
            shipmentSchema,
            Map("columnNameOfCorruptRecord" -> "_corrupt_record")
          )
        )

      // STEP 4: Extract and validate shipment fields

      val classifiedEvents = parsedEvents
        .withColumn("shipment_id", col("shipment.shipment_id"))
        .withColumn("event_type", col("shipment.event_type"))
        .withColumn("location", col("shipment.location"))
        .withColumn(
          "event_time",
          to_timestamp(col("shipment.event_time"))
        )
        .withColumn(
          "validation_status",
          when(
            col("shipment._corrupt_record").isNotNull,
            "INVALID_JSON"
          )
            .when(
              col("shipment").isNull,
              "INVALID_JSON"
            )
            .when(
              col("shipment_id").isNull ||
                length(trim(col("shipment_id"))) === 0,
              "MISSING_SHIPMENT_ID"
            )
            .otherwise("VALID")
        )
        .withColumn(
          "error_reason",
          when(
            col("validation_status") === "INVALID_JSON",
            "Malformed or unparseable JSON"
          )
            .when(
              col("validation_status") === "MISSING_SHIPMENT_ID",
              "shipment_id is null or blank"
            )
            .otherwise(lit(null).cast(StringType))
        )
        .withColumn("processed_at", current_timestamp())
        .drop("shipment")

      // STEP 5: Process each micro-batch

      val query = classifiedEvents.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpointLocation)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

          println(s"Processing micro-batch: $batchId")

          // Kafka partition + offset uniquely identifies an event
          // within this fixed Kafka topic.

          val deduplicated = batchDF
            .dropDuplicates("kafka_partition", "kafka_offset")
            .persist()

          try {

            val validEvents = deduplicated
              .filter(col("validation_status") === "VALID")
              .select(
                col("shipment_id"),
                col("event_type"),
                col("location"),
                col("event_time"),
                col("validation_status"),
                col("event_json"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp"),
                col("processed_at")
              )

            val invalidEvents = deduplicated
              .filter(col("validation_status") =!= "VALID")
              .select(
                col("shipment_id"),
                col("event_type"),
                col("location"),
                col("event_time"),
                col("validation_status"),
                col("error_reason"),
                col("event_json"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp"),
                col("processed_at")
              )

            val validCount = validEvents.count()
            val invalidCount = invalidEvents.count()

            println(s"Valid events: $validCount")
            println(s"Invalid events: $invalidCount")

            // STEP 6: MERGE valid events into accepted table

            if (validCount > 0) {

              validEvents.createOrReplaceGlobalTempView(
                "shipment_events_valid_source"
              )
              spark.sql(
                s"""
                   |MERGE INTO $acceptedTable AS t
                   |USING global_temp.shipment_events_valid_source AS s
                   |ON t.kafka_partition = s.kafka_partition
                   |AND t.kafka_offset = s.kafka_offset
                   |WHEN NOT MATCHED THEN
                   |INSERT (
                   |  shipment_id,
                   |  event_type,
                   |  location,
                   |  event_time,
                   |  validation_status,
                   |  event_json,
                   |  kafka_partition,
                   |  kafka_offset,
                   |  kafka_timestamp,
                   |  processed_at
                   |)
                   |VALUES (
                   |  s.shipment_id,
                   |  s.event_type,
                   |  s.location,
                   |  s.event_time,
                   |  s.validation_status,
                   |  s.event_json,
                   |  s.kafka_partition,
                   |  s.kafka_offset,
                   |  s.kafka_timestamp,
                   |  s.processed_at
                   |)
                   |""".stripMargin
              )

              println("Accepted events MERGE completed.")
            }

            // STEP 7: MERGE invalid events into quarantine

            if (invalidCount > 0) {

              invalidEvents.createOrReplaceGlobalTempView(
                "shipment_events_quarantine_source"
              )

              spark.sql(
                s"""
                   |MERGE INTO $quarantineTable AS t
                   |USING global_temp.shipment_events_quarantine_source AS s
                   |ON t.kafka_partition = s.kafka_partition
                   |AND t.kafka_offset = s.kafka_offset
                   |WHEN NOT MATCHED THEN
                   |INSERT (
                   |  shipment_id,
                   |  event_type,
                   |  location,
                   |  event_time,
                   |  validation_status,
                   |  error_reason,
                   |  event_json,
                   |  kafka_partition,
                   |  kafka_offset,
                   |  kafka_timestamp,
                   |  processed_at
                   |)
                   |VALUES (
                   |  s.shipment_id,
                   |  s.event_type,
                   |  s.location,
                   |  s.event_time,
                   |  s.validation_status,
                   |  s.error_reason,
                   |  s.event_json,
                   |  s.kafka_partition,
                   |  s.kafka_offset,
                   |  s.kafka_timestamp,
                   |  s.processed_at
                   |)
                   |""".stripMargin
              )

              println("Quarantine events MERGE completed.")
            }

            println(s"Micro-batch $batchId completed successfully.")

          } finally {
            deduplicated.unpersist()
          }
        }
        .start()

      // STEP 8: Event-time window analytics with watermarking

      val analyticsQuery = classifiedEvents
        .filter(
          col("validation_status") === "VALID" &&
            col("event_time").isNotNull &&
            col("event_type").isNotNull
        )
        .withWatermark("event_time", "2 minutes")
        .groupBy(
          window(col("event_time"), "1 minute").alias("event_window"),
          col("event_type")
        )
        .count()
        .select(
          col("event_window.start").alias("window_start"),
          col("event_window.end").alias("window_end"),
          col("event_type"),
          col("count").cast("long").alias("event_count")
        )
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", analyticsCheckpointLocation)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .foreachBatch { (windowDF: DataFrame, batchId: Long) =>

          println(s"Analytics micro-batch: $batchId")

          windowDF.createOrReplaceGlobalTempView(
            "shipment_event_window_source"
          )

          spark.sql(
            """
              |MERGE INTO meridian_adls.gold.shipment_event_window_metrics AS t
              |USING global_temp.shipment_event_window_source AS s
              |ON t.window_start = s.window_start
              |AND t.window_end = s.window_end
              |AND t.event_type = s.event_type
              |WHEN MATCHED THEN UPDATE SET
              |  t.event_count = s.event_count
              |WHEN NOT MATCHED THEN INSERT (
              |  window_start,
              |  window_end,
              |  event_type,
              |  event_count
              |)
              |VALUES (
              |  s.window_start,
              |  s.window_end,
              |  s.event_type,
              |  s.event_count
              |)
            """.stripMargin
          )

          println("Gold window metrics MERGE completed.")
        }
        .start()


      println("Meridian Iceberg Streaming started successfully.")
      println(s"Kafka topic: $topic")
      println(s"Checkpoint: $checkpointLocation")

      spark.streams.awaitAnyTermination()

    } finally {
      spark.stop()
    }
  }
}