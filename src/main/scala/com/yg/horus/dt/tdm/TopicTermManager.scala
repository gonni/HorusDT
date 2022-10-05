package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.SparkJobInit
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
//    joinedData.show()
//    println("up data is joined ..")

    val winFunc = Window.partitionBy("TOPIC_NO").orderBy($"SCORE".desc)

    stopRemovedData.withColumn("row", row_number.over(winFunc))
      .where($"row" < eachLimit)
  }

  def getTopicsSeq(limit: Int) = {
    getTopics(eachLimit = limit).map(_.getAs[String]("TERM")).collect().toSeq
  }
}

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

    println("latestGrpValue =>" + test.getLatestTopicGrp())

    println("Result -------------------- ")
    val arr = test.getTopics(eachLimit = 3).map(_.getAs[String]("TERM")).collect().toSeq
    arr.foreach(println)
  }
}
