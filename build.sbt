ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "meridian-iceberg",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.1.3" % Provided
    )
  )
