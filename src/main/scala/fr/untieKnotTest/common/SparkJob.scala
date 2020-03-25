package fr.untieKnotTest.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

trait SparkJob {

  val jobName = "producerJob"

  def sparkExtraParams: Set[String] = Set()

  val sparkEnvironment = new SparkEnv(jobName, sparkExtraParams)
  lazy val spark: SparkSession = sparkEnvironment.spark



}
