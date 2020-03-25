package fr.untieKnotTest.common

import java.util.Properties

import org.apache.spark._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}


class SparkEnv(name:String,extraConfKeys: Set[String] = Set()) {

  import scala.collection.JavaConverters._

  private val config: Config =  ConfigFactory.load("spark").withFallback(ConfigFactory.load())

  private val sparkConfig = {
    val self = new SparkConf().setAppName(name)
    val allConfKeys = extraConfKeys.toList :+ "spark"

    allConfKeys.foreach(
      confKey => config.getConfig(confKey).entrySet().asScala.map(x => (x.getKey, x.getValue.unwrapped().toString)).foreach {
        case (key, value) =>
          self.set(confKey + "." + key, value)
      }
    )
    self
  }

  lazy val spark: SparkSession = builder.getOrCreate()
  protected def builder = SparkSession.builder.config(sparkConfig) //.enableHiveSupport()

  val sc = spark.sparkContext

  val ssc: StreamingContext = {
    new StreamingContext(sc, Seconds(1))
  }

  ssc.checkpoint("checkpoint-directory")






}
