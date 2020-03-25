package fr.untieKnotTest.test

import java.nio.file.{Path, Paths}

import fr.untieKnotTest.kafka.Domains.Data
import org.scalatest.{FeatureSpec, FunSuite, Matchers}
import fr.untieKnotTest.kafka.FirstScript.writeToKafka
import fr.untieKnotTest.kafka.UDF.Udf._
import fr.untieKnotTest.kafka.Utils.JsonUtil
import fr.untieKnotTest.common.SparkJob
import fr.untieKnotTest.kafka.{FirstScript, FourthScript, SecondScript, ThirdScript}



class KafkaScriptSpec extends  FeatureSpec with Matchers with SparkJob {




  feature("Script 1") {

    scenario(" first Script :  Open a directory and split file before sending them to Kafka Queue") {

      FirstScript.main(Array.empty[String])




    }

  }

  feature("Script 2") {

    scenario(" Load A List of topics, read qOne with SparkStreaming") {

      SecondScript.main(Array.empty[String])




    }

  }

  feature("Script 3") {

    scenario(" Read qtwo and qtree since last offset and save its contend") {

     ThirdScript.main(Array.empty[String])




    }

  }
    feature("Script 4") {

      scenario(" Do Various Data Analysis ") {

        val pourcentage = "25"

        FourthScript.main(Array(pourcentage))




      }

}

}





