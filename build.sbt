ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "meridian-iceberg",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.1.3" % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.1.3" % Provided,
      "org.apache.kafka" % "kafka-clients" % "3.9.1"
    )
  )