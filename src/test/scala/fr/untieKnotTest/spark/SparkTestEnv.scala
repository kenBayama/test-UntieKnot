package fr.untieKnotTest.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkTestEnv {

  val conf = new SparkConf()
  conf.setMaster("local[2]")
  conf.setAppName("UnitTests")
  conf.set("spark.sql.shuffle.partitions", "2")
  val spark = SparkSession.builder().config(conf).getOrCreate()
}
