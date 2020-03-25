package fr.untieKnotTest.kafka

import java.nio.file.Paths

import fr.untieKnotTest.common.SparkJob

object ThirdScript extends SparkJob{

  def main (arg:Array[String]): Unit = {

    import spark.implicits._

    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("subscribe","qtree")
      .option("startingOffsets", "earliest")
      .load()
      .writeStream
      .format("parquet")
      .option("path",Paths.get("src/test/resources/scriptQ3/").toAbsolutePath.toString())
      .option("checkpointLocation", "checkpointScript3_Q3")
      .start()
      .awaitTermination(30000)



    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("subscribe","result")
      .option("startingOffsets", "earliest")
      .load()
      .writeStream
      .format("parquet")
      .option("path",Paths.get("src/test/resources/scriptQ2/").toAbsolutePath.toString())
      .option("checkpointLocation", "checkpointScript3_Q2")
      .start()
      .awaitTermination(30000)





  }

  }
