
package com.meridian

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object ShipmentEventStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Meridian-Shipment-Event-Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Step 1: Define shipment JSON schema
    val shipmentSchema = StructType(Seq(
      StructField("shipment_id", StringType, true),
      StructField("event_type", StringType, true),
      StructField("location", StringType, true),
      StructField("event_time", StringType, true),
      StructField("_corrupt_record", StringType, true)
    ))

    // Step 2: Read events from Kafka
    val events = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "meridian-shipment-events")
      .option("startingOffsets", "latest")
      .load()

    // Step 3: Preserve raw event and Kafka metadata
    val rawEvents = events.select(
      col("value").cast("string").alias("event_json"),
      col("partition").alias("kafka_partition"),
      col("offset").alias("kafka_offset")
    )

    // Step 4: Parse JSON
    val parsedEvents = rawEvents
      .withColumn(
        "shipment",
        from_json(
          col("event_json"),
          shipmentSchema,
          Map("columnNameOfCorruptRecord" -> "_corrupt_record")
        )
      )

    // Step 5: Extract fields and classify events
    val classifiedEvents = parsedEvents
      .withColumn(
        "shipment_id",
        col("shipment.shipment_id")
      )
      .withColumn(
        "event_type",
        col("shipment.event_type")
      )
      .withColumn(
        "location",
        col("shipment.location")
      )
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
      .drop("shipment")

    // Step 6: Display classified events
    val query = classifiedEvents.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "/tmp/meridian-checkpoints/shipment-events-validation"
      )
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    println("Meridian Shipment Validation Streaming started successfully.")

    query.awaitTermination()
  }
}
