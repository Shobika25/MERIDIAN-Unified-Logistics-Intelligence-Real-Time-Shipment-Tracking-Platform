
package com.meridian

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CreateUpdateTestSource {

  def main(args: Array[String]): Unit = {

    val sourcePath =
      "abfss://meridian@adls2501.dfs.core.windows.net/exchange/fact_shipment_export/"

    val testPath =
      "/tmp/meridian-batch-update-test/source"

    val spark = SparkSession.builder()
      .appName("Meridian-Create-Update-Test-Source")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {

      val source = spark.read.parquet(sourcePath)

      val updatedSource = source.withColumn(
        "freight_charge_inr",
        when(
          col("shipment_id") === "SHP2026008477",
          lit(450.60)
        ).otherwise(col("freight_charge_inr"))
      )

      updatedSource.write
        .mode("overwrite")
        .parquet(testPath)

      println("UPDATE TEST SOURCE CREATED")

      spark.read.parquet(testPath)
        .filter(col("shipment_id") === "SHP2026008477")
        .select("shipment_id", "freight_charge_inr")
        .show(false)

      println(s"Total test rows: ${spark.read.parquet(testPath).count()}")

    } finally {
      spark.stop()
    }
  }
}
