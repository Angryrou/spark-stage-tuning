name := "spark-stage-tuning"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.17"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"
libraryDependencies += "com.databricks" %% "spark-sql-perf" % "0.5.1-SNAPSHOT" from (s"file://${baseDirectory.value.getAbsolutePath}/benchmark-res/libs/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar")
