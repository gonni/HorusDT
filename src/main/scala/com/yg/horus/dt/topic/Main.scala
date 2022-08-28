package com.yg.horus.dt.topic

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

object Main {
  def main(args: Array[String]): Unit = {
    println("Active System ..")
    val conf = new SparkConf().setMaster("local[*]").setAppName("text lda")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new LdaTopicProcessing(spark)
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(10))
    val source = test.loadSource(21L, fromTime)

    source.show()

    println("----- Topic terms -----")
    val topics = test.topics(source, 10, 10)
    val fRes = test.convertObject(topics)

    for(i <- 0 until fRes.length) {
      println(s"Topic #${i}")
      fRes(i).foreach(a => println(a))
      println("--------------")
    }

    test.saveToDB(topics, 21L, 30)

//    println("Size => " + source.collect().size)
  }
}
