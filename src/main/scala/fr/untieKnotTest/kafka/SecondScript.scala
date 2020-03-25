package fr.untieKnotTest.kafka

import fr.untieKnotTest.common.SparkJob
import fr.untieKnotTest.kafka.Domains.Schema.dataSchema
import fr.untieKnotTest.kafka.Domains.{DataPrepared, QueueUniqueData, TopicData}
import fr.untieKnotTest.kafka.UDF.Udf.splitStringInListBySpaceUDF
import org.apache.spark.sql.functions._

object SecondScript extends SparkJob {

  def main(args:Array[String]) ={

    import spark.implicits._

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

    val seqTopic =Seq(sport,jobs)


    val dstream1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("subscribe","qOne")
      .option("startingOffsets", "earliest")
      .load()
      .select($"value" cast "string")


    val formatted = dstream1.select(
      from_json(col("value"), dataSchema).alias("data"))
      .select("data.*")
      .withColumn("word",splitStringInListBySpaceUDF($"word"))
      .as[DataPrepared]
      .flatMap { x =>
        seqTopic.flatMap(y => y.list.foldLeft(Seq.empty[QueueUniqueData])( (mp, s) => {
          val res =
            x.word.contains(y.name) match {
              case true => Some(Seq((QueueUniqueData(x.source,  y.name, y.name, true))))
              case false => {
                x.word.contains(s) match {
                  case true => Some(Seq((QueueUniqueData(x.source, s, y.name, false))))
                  case false => None
                }

              }

            }
          mp ++ res.getOrElse(Seq.empty[QueueUniqueData])
        }))
      }.as [QueueUniqueData]
      .groupBy("source","word")
      .agg(
        collect_set("topics") as ("topics"),
        first(col("word")) as("topic"),
        first(
          when(col("isNameTopic")=== true,
            lit("qtree")).otherwise(lit("qtwo"))) as ("topicFinal"))
      .withColumn("value",when(col("topicFinal") =!= lit("qtwo"),
        to_json(struct("source","topic")))
        .otherwise(to_json(struct("source","word","topics"))))
      .select($"value", $"topicFinal".as("topic"))


    .writeStream
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("checkpointLocation", "checkpointScript2")
      .start()
      .awaitTermination(30000)






  }


}
