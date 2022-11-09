package com.yg.horus.dt.termcount

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.yg.horus.conn.InfluxClient
import com.yg.horus.dt.SparkStreamingInit


object MultiMain extends SparkStreamingInit("STREAM_TC") {


  def processCrawled(seedIds: Seq[Long]) = {
    val anchors = ssc.receiverStream(new TimePeriodMysqlSourceReceiver(seedIds))
//    anchors.print()

    seedIds.foreach(seedId => {
      val wordCounts = anchors.filter(a => a._1 == seedId).flatMap(an => {
        HangleTokenizer().arrayNouns(an._2)
      }).map(word => (word, 1)).reduceByKey(_ + _)

      wordCounts.print()

//      wordCounts.foreachRDD( wc => {
//          InfluxClient.writeTf(seedId,)
//        }
//      )
    })

    //---------
//    val words = anchors.flatMap(anchor => {
//      HangleTokenizer().arrayNouns(anchor._2)
//    })
//
//
//
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//
//    wordCounts.print

//    wordCounts.foreachRDD(rdd => {
//      rdd.foreach(tf => {
//        InfluxClient.writeTf(anchors., tf._1, tf._2)
//      })
//    })

  }


  def main(v: Array[String]): Unit = {
    println("Active System ..")

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig("profile.name"))
    println("------------------------------------------------")
    println("RuntimeConfig Details : " + RuntimeConfig())

    if (v.length > 0) {
      println("Count of Input Arguments => " + v.length)
      val seeds = v.map(args => {
        args.toLong
      }).toSeq

      processCrawled(seeds)
    } else {
      println("No input arguments or Invalid type arguments detected ..")

      val seeds = Seq(1L, 2L)
      processCrawled(seeds)

    }

    ssc.start()
    ssc.awaitTermination()

  }
}
