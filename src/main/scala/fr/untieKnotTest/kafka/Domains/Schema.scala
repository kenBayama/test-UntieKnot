package fr.untieKnotTest.kafka.Domains

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructField, StructType}

object Schema {

val dataSchema = StructType(
    Seq(
  StructField("source",StringType,false),
  StructField("word",StringType,false)))



val dataQueue3 = StructType(
  Seq(
StructField("source",StringType,false),
StructField("topic",StringType,false)))



val dataQueue2 = StructType(
Seq(
  StructField("source",StringType,false),
  StructField("word",StringType,false),
  StructField("topics",ArrayType(StringType,false),false))
)



}