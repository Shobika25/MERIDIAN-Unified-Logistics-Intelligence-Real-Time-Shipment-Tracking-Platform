

package com.meridian

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FactShipmentIcebergBatch {

  private val defaultSourcePath =
    "abfss://meridian@adls2501.dfs.core.windows.net/exchange/fact_shipment_export/"

  private val defaultTargetTable =
    "meridian_adls.gold.fact_shipment"

  private val columns = Seq(
    "shipment_id",
    "customer_key",
    "carrier_key",
    "origin_hub_key",
    "destination_hub_key",
    "status_key",
    "ship_date_key",
    "promised_date_key",
    "delivery_date_key",
    "ship_date",
    "promised_date",
    "delivery_date",
    "weight_kg",
    "distance_km",
    "freight_charge_inr",
    "transit_days",
    "on_time_delivery_flag",
    "tracking_event_count",
    "hub_stops_count",
    "total_delay_minutes",
    "last_event_time",
    "shipment_status"
  )

  def main(args: Array[String]): Unit = {

    val sourcePath =
      args.lift(0).getOrElse(defaultSourcePath)

    val targetTable =
      args.lift(1).getOrElse(defaultTargetTable)

    require(
      targetTable.matches("[A-Za-z0-9_.]+"),
      "Invalid target table identifier"
    )

    val spark = SparkSession.builder()
      .appName("Meridian-FactShipment-Iceberg-Batch")
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

      println("========== MERIDIAN ICEBERG BATCH ==========")
      println(s"Source: $sourcePath")
      println(s"Target: $targetTable")

      // STEP 1: Read source

      println("\nSTEP 1: Reading ADLS Parquet source")

      val source = spark.read
        .format("parquet")
        .load(sourcePath)

      val actualColumns = source.columns.toSeq

      val missingColumns = columns.diff(actualColumns)

      require(
        missingColumns.isEmpty,
        s"Missing source columns: ${missingColumns.mkString(", ")}"
      )

      require(
        actualColumns.toSet == columns.toSet,
        "Source contains unexpected or missing columns"
      )

      val sourceCount = source.count()

      require(sourceCount > 0, "Source dataset is empty")

      println(s"Source rows: $sourceCount")
      println(s"Source columns: ${actualColumns.size}")

      // STEP 2: Validate shipment IDs

      println("\nSTEP 2: Validating shipment IDs")

      val nullIdCount = source
        .filter(col("shipment_id").isNull)
        .count()

      require(
        nullIdCount == 0,
        s"Found $nullIdCount null shipment IDs"
      )

      val duplicateCount = source
        .groupBy("shipment_id")
        .count()
        .filter(col("count") > 1)
        .count()

      require(
        duplicateCount == 0,
        s"Found $duplicateCount duplicate shipment IDs"
      )

      println("Null shipment IDs: 0")
      println("Duplicate shipment IDs: 0")

      // STEP 3: Validate Iceberg target

      println("\nSTEP 3: Checking Iceberg target")

      val target = spark.table(targetTable)

      require(
        target.columns.toSeq == columns,
        "Target Iceberg table schema does not match expected schema"
      )

      val beforeCount = target.count()

      println(s"Target rows before processing: $beforeCount")

      // STEP 4: Register source view

      source.createOrReplaceTempView("fact_shipment_source")

      // STEP 5: Detect changed or new records

      println("\nSTEP 4: Detecting new or changed shipments")

      val changeCondition = columns
        .filterNot(_ == "shipment_id")
        .map(c => s"NOT (t.`$c` <=> s.`$c`)")
        .mkString(" OR ")

      val changeDetectionSql =
        s"""
           |SELECT COUNT(*)
           |FROM fact_shipment_source AS s
           |LEFT JOIN $targetTable AS t
           |  ON t.shipment_id = s.shipment_id
           |WHERE t.shipment_id IS NULL
           |   OR ($changeCondition)
           |""".stripMargin

      val changedOrNewCount =
        spark.sql(changeDetectionSql).first().getLong(0)

      println(s"New or changed shipments: $changedOrNewCount")

      // STEP 6: Incremental MERGE

      if (changedOrNewCount == 0) {

        println(
          "No new or changed shipments detected. Skipping Iceberg MERGE."
        )

      } else {

        println("\nSTEP 5: Executing incremental Iceberg MERGE")

        val updateAssignments = columns
          .filterNot(_ == "shipment_id")
          .map(c => s"t.`$c` = s.`$c`")
          .mkString(", ")

        val insertColumns = columns
          .map(c => s"`$c`")
          .mkString(", ")

        val insertValues = columns
          .map(c => s"s.`$c`")
          .mkString(", ")

        val mergeSql =
          s"""
             |MERGE INTO $targetTable AS t
             |USING fact_shipment_source AS s
             |ON t.shipment_id = s.shipment_id
             |WHEN MATCHED AND ($changeCondition)
             |THEN UPDATE SET $updateAssignments
             |WHEN NOT MATCHED
             |THEN INSERT ($insertColumns)
             |VALUES ($insertValues)
             |""".stripMargin

        spark.sql(mergeSql)

        println("MERGE completed successfully")
      }

      // STEP 7: Validate final target

      println("\nSTEP 6: Validating Iceberg target")

      val finalTarget = spark.table(targetTable)

      val afterCount = finalTarget.count()

      val distinctIds = finalTarget
        .select("shipment_id")
        .distinct()
        .count()

      val nullTargetIds = finalTarget
        .filter(col("shipment_id").isNull)
        .count()

      require(
        nullTargetIds == 0,
        s"Found $nullTargetIds null shipment IDs in target"
      )

      require(
        afterCount == distinctIds,
        "Duplicate shipment IDs found in target"
      )

      val metrics = finalTarget.agg(
        count(lit(1)).as("total_shipments"),
        sum("tracking_event_count").as("tracking_events"),
        sum("hub_stops_count").as("hub_stops"),
        sum("total_delay_minutes").as("delay_minutes")
      ).first()

      println(s"Rows before processing: $beforeCount")
      println(s"Rows after processing: $afterCount")
      println(s"Distinct shipment IDs: $distinctIds")
      println(s"New or changed source shipments: $changedOrNewCount")
      println(s"Total tracking events: ${metrics.getAs[Long]("tracking_events")}")
      println(s"Total hub stops: ${metrics.getAs[Long]("hub_stops")}")
      println(s"Total delay minutes: ${metrics.getAs[Long]("delay_minutes")}")

      // STEP 8: Display latest Iceberg snapshot

      println("\nSTEP 7: Latest Iceberg snapshot")

      spark.sql(
        s"""
           |SELECT committed_at, snapshot_id, operation
           |FROM $targetTable.snapshots
           |ORDER BY committed_at DESC
           |LIMIT 1
           |""".stripMargin
      ).show(false)

      println("\n========== BATCH COMPLETED SUCCESSFULLY ==========")

    } finally {
      spark.stop()
    }
  }
}
