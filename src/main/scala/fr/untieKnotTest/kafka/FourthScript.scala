package fr.untieKnotTest.kafka

import java.nio.file.Paths

import fr.untieKnotTest.common.SparkJob
import fr.untieKnotTest.kafka.Domains.{QueueData, TopicData}
import fr.untieKnotTest.kafka.Domains.Schema._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FourthScript extends SparkJob{

  def main (args:Array[String]): Unit = {

    val jobs = TopicData("Jobs",Seq("pipelayer","archivist","knight","manager","major","announcer",
      "candidate","tutor","cobbler","sailor","soldier","astronomer","accountant","taxidriver","paver",
      "policeman","chemist","don","realtor","academic","developer","millwright","canon","butcher",
      "barman","employee","fabricator","clerk","pedlar","player","analyst","labourer",
      "singer","baron","pilot","hygienist","lecturer","musician","geologist","machinist",
      "nurse","magician","secretary","architect","midwife","piper","brewer","advisor","dean","librarian",
      "soccer player","jogger","swimmer"))

    val sport = TopicData("Sport",Seq("running","score","epee","golf","parkour","skier","pingpong",
      "dugout","jogger","toboggan","batter","archery","movement","target","swimmer","bowling","wicket",
      "helmet","cycling","trampoline","lacrosse","swimming","strike","go","scuba","teammate",
      "goldmedal","puck","waterpolo","dartboard","runner","winner","bobsleigh","jumping",
      "referee","arrow","relay","medal","pentathlon","home","soccer player","goalie","discus",
      "bicycling","triathlon","scoreboard","gymnasium","racer","kingfu","sport"))

   val dfQ3 =  spark.read.parquet(Paths.get("src/test/resources/scriptQ3/").toAbsolutePath.toString())
      .select(
        from_json(col("value") cast("string"), dataQueue3).alias("data"))
      .select("data.*")


    val dfQ2 =  spark.read.parquet(Paths.get("src/test/resources/scriptQ2/").toAbsolutePath.toString())
       .select(
         from_json(col("value") cast("string"), dataQueue2).alias("data"))
       .select("data.*").distinct()
      .filter(col("topics").isNotNull)
      .withColumn("totalKeywordPerTopic",
        when(expr("topics[0]") === lit("Sport")
          && size(col("topics")) =!= 2 ,sport.list.size)
          .otherwise(
            when(expr("topics[0]") === lit("Jobs")
              && size(col("topics")) =!= 2 ,jobs.list.size).otherwise(lit(3))))

      .groupBy(col("topics"),col("source"))

      .agg(first("totalKeywordPerTopic"),
        count("word") as ("nbWordPerSource"),
        ( (count("word")
          /first("totalKeywordPerTopic"))*100)
          as("pourcentage")

      ).sort(asc("nbWordPerSource"))
      .withColumn("falsePositif",
        when(col("pourcentage") < lit(args(0).toDouble),true)
        .otherwise(false))

      .show(10000,false)

    //args(0).toInt


  }

}
