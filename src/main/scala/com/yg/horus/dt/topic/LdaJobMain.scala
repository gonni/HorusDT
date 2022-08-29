package com.yg.horus.dt.topic

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

object LdaJobMain {
  case class RunParams(appName: String, master: String, seedNo: Long, minAgo: Int,
                       cntTopic: Int = 10, cntTopicTerms: Int = 10)

  def main(args: Array[String]): Unit = {
    println("--------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("--------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())

    val rtParam = args.length match {
      case 6 => RunParams(args(0), args(1), args(2).toLong, args(3).toInt, args(4).toInt, args(5).toInt)
      case _ => RunParams("LDA_TOPIC", RuntimeConfig("spark.master"), 1L, 60)
    }

    println(s"Applied Params : ${rtParam}")

    val conf = new SparkConf().setMaster(rtParam.master).setAppName(rtParam.appName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new LdaTopicProcessing(spark)
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(rtParam.minAgo))
    val source = test.loadSource(rtParam.seedNo, fromTime)

    source.show()

    println("----- Topic terms -----")
    val topics = test.topics(source, 10, 15)
    val fRes = test.convertObject(topics)

    for(i <- 0 until fRes.length) {
      println(s"Topic #${i}")
      fRes(i).foreach(a => println(a))
      println("--------------")
    }

    test.saveToDB(topics, rtParam.seedNo, rtParam.minAgo)

    spark.close()
  }
}
