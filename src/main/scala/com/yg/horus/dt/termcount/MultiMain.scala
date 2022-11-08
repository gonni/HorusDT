package com.yg.horus.dt.termcount

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.yg.horus.conn.InfluxClient

object MultiMain {

  def processCrawled(ssc: StreamingContext, seedIds: Seq[Long]) = {
    val anchors = ssc.receiverStream(new TimePeriodMysqlSourceReceiver(seedIds))

    for(seed <- seedIds ) {
      val words = anchors.flatMap(anchor => {
        HangleTokenizer().arrayNouns(anchor._2)
      })

      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      wordCounts.print

      wordCounts.foreachRDD(rdd => {
        rdd.foreach(tf => {
          InfluxClient.writeTf(seedId, tf._1, tf._2)
        })
      })
    }

  }

  def processCrawleds(ssc: StreamingContext, seedIds: Seq[Long]) = {
    seedIds.foreach(seedId => {
      processCrawled(ssc, seedId)
    }
    )
  }

  def main(v: Array[String]): Unit = {
    println("Active System ..")

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig("profile.name"))
    println("------------------------------------------------")
    println("RuntimeConfig Details : " + RuntimeConfig())

    val conf = new SparkConf()
      .setMaster(RuntimeConfig("spark.master"))
      .setAppName("MULTI-TERM-COUNT-STREAM")
    val ssc = new StreamingContext(conf, Seconds(10))

    if (v.length > 0) {
      println("Count of Input Arguments => " + v.length)
      val seeds = v.map(args => {
        args.toLong
      }).toSeq

      processCrawleds(ssc, seeds)

      ssc.start()
    } else {
      println("No input arguments or Invalid type arguments detected ..")

//      val seeds = Seq(21L, 23L, 25L)
//      processCrawleds(ssc, seeds)
//
//      ssc.start()
    }
    ssc.awaitTermination()
  }
}
