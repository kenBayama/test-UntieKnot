package fr.untieKnotTest.kafka.UDF

import org.apache.spark.sql.functions.udf

object Udf {


  val getFileName : String => String = { s =>
   s.split("/").last
  }

  val getFileNameUDF = udf(getFileName)


  val getTopicName : String => String = { s =>
    s.split("/").last.replace(".csv","")
  }

  val getTopicNameUDF = udf(getTopicName)



  val splitStringInList : String => Seq[String] = {
    s => s.split(",")

  }

  val splitStringInListUDF = udf(splitStringInList)

  val splitStringInListBySpace : String => Seq[String] = {
    s => Option(s).getOrElse("").replace(".","").split(" ")

  }

  val splitStringInListBySpaceUDF = udf(splitStringInListBySpace)
}
