
package com.meridian

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CreateInsertTestSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Meridian Insert Test Source")
      .getOrCreate()

    val sourcePath =
      "file:///tmp/meridian-batch-update-test/source"

    val outputPath =
      "/tmp/meridian-batch-insert-test/source"

    val testShipmentId = "SHP_TEST_INSERT_0001"

    println("========== INSERT TEST SOURCE CREATION ==========")

    val existing = spark.read.parquet(sourcePath)

    require(
      !existing.filter(col("shipment_id") === testShipmentId).head(1).nonEmpty,
      s"Test shipment ID already exists: $testShipmentId"
    )

    val newShipment = existing
      .limit(1)
      .withColumn("shipment_id", lit(testShipmentId))

    val insertSource = existing.unionByName(newShipment)

    insertSource.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"Original rows: ${existing.count()}")
    println(s"New shipment ID: $testShipmentId")
    println(s"Final source rows: ${insertSource.count()}")

    insertSource
      .filter(col("shipment_id") === testShipmentId)
      .select("shipment_id", "freight_charge_inr")
      .show(false)

    println("INSERT TEST SOURCE CREATED SUCCESSFULLY")

    spark.stop()
  }
}
