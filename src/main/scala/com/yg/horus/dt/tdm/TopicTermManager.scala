package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.SparkJobInit
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

class TopicTermManager(spark: SparkSession) {
  import spark.implicits._

  val prop = new Properties()
  prop.put("user", RuntimeConfig().getString("mysql.user"))
  prop.put("password", RuntimeConfig().getString("mysql.password"))

  def getLatestTopicGrp() = {

    val ldaTable = spark.read.jdbc(RuntimeConfig("mysql.url"), "DT_LDA_TOPICS", prop)
    val maxGrp = ldaTable.orderBy($"GRP_TS".desc).take(1).headOption.get.getAs[Long]("GRP_TS")
//    println("MaxGrpValue -> " + maxGrp)
    maxGrp
  }

  def getTopics(grpTs: Long = getLatestTopicGrp(), eachLimit: Int) = {
    val ldaTable = spark.read.jdbc(RuntimeConfig("mysql.url"), "DT_LDA_TOPICS", prop)
    val latestData = ldaTable.filter($"GRP_TS" === grpTs)

    val winFunc = Window.partitionBy("TOPIC_NO").orderBy($"SCORE".desc)

    latestData.withColumn("row", row_number.over(winFunc))
      .where($"row" < eachLimit)
  }
}

object TopicTermManager extends SparkJobInit("TOPIC_TERM") {
  def main(args: Array[String]): Unit = {
    println("Active System ..")

    val test = new TopicTermManager(spark)
    println(" =>" + test.getLatestTopicGrp())

    println("Result -------------------- ")
    test.getTopics(eachLimit = 2).show()

  }
}
