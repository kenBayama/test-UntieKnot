package fr.untieKnotTest.kafka
import java.io._
import java.nio.file.Paths

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Utils {


  import org.apache.commons.io.FileUtils


  def cleanAllDirectories: Unit = {


  Seq("scriptQ2","scriptQ3").map (x =>
    FileUtils.cleanDirectory(Paths.get("src/test/resources/"+x+"/").toAbsolutePath.toFile))

  Seq("checkpoint-directory","checkpointScript2","checkpointScript3_Q2","checkpointScript3_Q3")
    .map(  x=>  FileUtils.cleanDirectory(Paths.get(x).toAbsolutePath.toFile))

}

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
