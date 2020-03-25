package fr.untieKnotTest.kafka

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Utils {

  object JsonUtil {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    // .setSerializationInclusion(Include.NON_NULL)
    // .setSerializationInclusion(Include.NON_EMPTY)
    //.setSerializationInclusion(Include.NON_ABSENT)


    def toJson(value: Any): String = {
      //println(mapper.writeValueAsString(value))
      mapper.writeValueAsString(value)
    }
  }


}
