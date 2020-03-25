name := "test-UntieKnot"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies in ThisBuild ++= Seq (

  "org.apache.kafka" %% "kafka" % "2.1.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark"  %% "spark-core" % sparkVersion exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0",
 "com.github.kxbmap" % "configs_2.11" % "0.4.4",
  "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.7.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.3",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.2"



)

