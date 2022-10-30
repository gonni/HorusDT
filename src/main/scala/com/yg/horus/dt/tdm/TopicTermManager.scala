package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.SparkJobInit
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

class TopicTermManager(spark: SparkSession) {
  import spark.implicits._

  val prop = new Properties()
  prop.put("user", RuntimeConfig().getString("mysql.user"))
  prop.put("password", RuntimeConfig().getString("mysql.password"))


  def getStopWord(minScore: Double) = {
    val termScoreTable = spark.read.jdbc(RuntimeConfig("mysql.url"), "DT_TERM_SCORE", prop)
    val stopWords = termScoreTable.filter($"AVG_TFIDF" < minScore).select($"TOKEN")
    stopWords
  }

  def getLatestTopicGrp() = {
    val ldaTable = spark.read.jdbc(RuntimeConfig("mysql.url"), "DT_LDA_TOPICS", prop)
    val maxGrp = ldaTable.orderBy($"GRP_TS".desc).take(1).headOption.get.getAs[Long]("GRP_TS")
//    println("MaxGrpValue -> " + maxGrp)
    maxGrp
  }

  def getTopics(grpTs: Long = getLatestTopicGrp(), eachLimit: Int,
                stopWords: DataFrame = getStopWord(0.6)) = {
    val ldaTable = spark.read.jdbc(RuntimeConfig("mysql.url"), "DT_LDA_TOPICS", prop)
    val latestData = ldaTable.filter($"GRP_TS" === grpTs)
    val joinedData = latestData.join(stopWords, latestData("TERM") === stopWords("TOKEN"), "left")
    val stopRemovedData = joinedData.filter($"TOKEN".isNull)

    val winFunc = Window.partitionBy("TOPIC_NO").orderBy($"SCORE".desc)

    stopRemovedData.withColumn("row", row_number.over(winFunc))
      .where($"row" < eachLimit)
  }

  def getTopicsSeq(limit: Int) = {
    getTopics(eachLimit = limit).map(_.getAs[String]("TERM")).collect().toSeq
  }

  def getLatestTopicsSeq(seedNo: Long, limit: Int) = {
    getTopics(grpTs = getLatestTopicGrp(seedNo), eachLimit = limit)
      .map(_.getAs[String]("TERM")).collect().toSeq
  }

  def getLatestTopicGrpSeq(seedNo: Long, limit: Int) = {
    getTopics(grpTs = getLatestTopicGrp(seedNo), eachLimit = limit)
  }

  def getLatestTopicGrp(seedNo: Long) = {
    val ldaTable = spark.read.jdbc(RuntimeConfig("mysql.url"), "DT_LDA_TOPICS", prop)
    val maxGrp = ldaTable.filter($"SEED_NO" === seedNo)
      .orderBy($"GRP_TS".desc).take(1).headOption.get.getAs[Long]("GRP_TS")
    maxGrp
  }

  def getTopicSeq(seedNo: Long, limit: Int) = {
    val grpTsVal = getLatestTopicGrp(seedNo)
    getTopics(grpTs = grpTsVal, eachLimit = limit).map(_.getAs[String]("TERM")).collect().toSeq
  }


  def getMergedTopicSeq(seedNo: Long, termLimit: Int = 5, limit: Int = 5) = {
    getLatestTopicGrpSeq(seedNo, termLimit)
      .groupBy($"TOPIC_NO")
      .agg(concat_ws("|", collect_list($"TERM")) as "GRP_TOPIC",
        sum($"SCORE"))
      .orderBy($"sum(SCORE)".desc)
      .limit(limit)
      .map(row => MergedTopic(
        row.getAs[Int]("TOPIC_NO"),
        row.getAs[String]("GRP_TOPIC"),
        row.getAs[Double]("sum(SCORE)"))).collect().toSeq
  }


}

case class MergedTopic(topicNo: Int, topicName: String, score: Double)

object TopicTermManager extends SparkJobInit("TOPIC_TERM") {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    val test = new TopicTermManager(spark)
//    println("stopWords => ")
    val stopWords = test.getStopWord(0.6)
//    val lstStopWords = stopWords.distinct().collectAsList()
//    lstStopWords.forEach(row => {
//      println(" -stopWords List->" + row.getString(0))
//    })

//    println("latestGrpValue =>" + test.getLatestTopicGrp(23L))

    println("Result -------------------- ")
//    val arr = test.getTopics(eachLimit = 3).map(_.getAs[String]("TERM")).collect().toSeq
//    arr.foreach(println)
//    test.getTopicSeq(23L, 10).foreach(println)

    // THIS!!
//    test.getLatestTopicGrpSeq(21L, 5)
//      .groupBy($"TOPIC_NO")
////      .agg(sum($"SCORE" as "TOTAL_SCORE"))
//      .agg(concat_ws("|", collect_list($"TERM")) as "GRP_TOPIC",
//        sum($"SCORE"))
//      .orderBy($"sum(SCORE)".desc)
//      .limit(10)
//      .show()
    val mergedTopics = test.getMergedTopicSeq(21L, 10, 10)
    mergedTopics.foreach(println)

//    val mergedTerms = mergedTopics.map(_.topicName)
    println("=============================")
//    test.getLatestTopicGrpSeq(21L, 5)
//      .groupBy($"TOPIC_NO")
//      .sum("SCORE")
//      .select($"TOPIC_NO", $"sum(SCORE)".alias("TOTAL_SCORE"))
//      .orderBy($"TOTAL_SCORE".desc)
//      .show()

    println("=============================")
//    test.getLatestTopicGrpSeq(21L, 5).groupBy($"TOPIC_NO").agg(sum($"SCORE" as "TOTAL_SCORE")) show

// ------------
    val model = Word2VecModel.load("data/w2vNews2Cont_v200_m8_w7_it8")

    val tt = new TdmMaker(spark, model)

    mergedTopics.foreach(mtpc => {
      val a = tt.nearTermsOnVectorIn(mtpc.topicName.split("\\|").toSeq, 7)
        .orderBy($"similarity".desc)
        .map(_.getAs[String]("word")).collect().mkString("|")
      println("nearTerm =>" + a)
      println("===========>" + mtpc)
    })

//    val x = "축제|행사|취소|대구|러"
//    x.split("\\|").foreach(println)

    // ------------

//    val nearTerms = tt.nearTermsOnVector(Seq("한국", "러시아", "우크라이나"), 10)
//    nearTerms.toDF().show()
  }
}
