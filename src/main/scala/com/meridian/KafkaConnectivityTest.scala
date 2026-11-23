package com.meridian

import org.apache.spark.sql.SparkSession

object KafkaConnectivityTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Meridian-Kafka-Connectivity-Test")
      .master("local[*]")
      .getOrCreate()

    try {
      val events = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "meridian-shipment-events")
        .option("startingOffsets", "earliest")
        .load()

      println("===== KAFKA MESSAGE COUNT =====")
      println(events.count())

      println("===== KAFKA EVENTS =====")

      events.selectExpr(
        "CAST(key AS STRING) AS shipment_key",
        "CAST(value AS STRING) AS event_json",
        "partition",
        "offset"
      ).show(20, false)

    } finally {
      spark.stop()
    }
  }
}