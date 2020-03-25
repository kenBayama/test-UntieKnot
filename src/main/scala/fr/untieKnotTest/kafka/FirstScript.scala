package fr.untieKnotTest.kafka

import java.nio.file.{Path, Paths}
import java.util.Properties
import org.apache.commons.io.FileUtils

import fr.untieKnotTest.common.SparkJob
import fr.untieKnotTest.kafka.Domains.Data
import fr.untieKnotTest.kafka.UDF.Udf.getFileNameUDF
import fr.untieKnotTest.kafka.Utils._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object FirstScript extends SparkJob{

  def writeToKafka(topic: String,data:String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, "key", data)
    val res = producer.send(record)
    producer.close()
    res
  }




  def main (arg:Array[String]) {

    import spark.implicits._

    val directory: Path
    = Paths.get("src/test/resources/script1/")

    val readFiles = spark.sparkContext.
      wholeTextFiles(directory.toAbsolutePath().toString())


    val dataToSend = spark.createDataFrame(readFiles
      .map { x => Data(x._1, x._2) }).as[Data]
      .withColumn("source", getFileNameUDF($"source"))
      .as[Data]
      .map(x => JsonUtil.toJson(x))
      .foreach(x => writeToKafka("qOne", x)
      )

    cleanAllDirectories


  }
}

