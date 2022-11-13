package com.yg.horus.dt.termcount

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.yg.horus.conn.InfluxClient

object MultiMain {


  def processCrawled(seedIds: Seq[Long])(implicit ssc: StreamingContext) = {
    val anchors = ssc.receiverStream(new TimePeriodMysqlSourceReceiver(seedIds))
//    anchors.print()

    seedIds.foreach(seedId => {
      val wordCounts = anchors.filter(a => a._1 == seedId).flatMap(an => {
        HangleTokenizer().arrayNouns(an._2)
      }).map(word => (word, 1)).reduceByKey(_ + _)
      println(s"------------- seedId# ${seedId} ---------------")
      wordCounts.print()

      wordCounts.foreachRDD( wc => {
        wc.foreach(tf => {
          InfluxClient.writeTf(seedId, tf._1, tf._2)
        })
        }
      )
    })
  }


  def main(v: Array[String]): Unit = {
    println("Active System ..")

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig("profile.name"))
    println("------------------------------------------------")
    println("RuntimeConfig Details : " + RuntimeConfig())

    val runtimeConf = RuntimeConfig.getRuntimeConfig()
    val conf = new SparkConf().setMaster(runtimeConf.getString("spark.master")).setAppName("STREAM-MULTI-SEEDS-TC")
    //  val spark = SparkSession.builder().config(conf).getOrCreate()
    implicit val ssc = new StreamingContext(conf, Seconds(10))

    if (v.length > 0) {
      println("Count of Input Arguments => " + v.length)
      val seeds = v.map(args => {
        args.toLong
      }).toSeq

      processCrawled(seeds)
    } else {
      println("No input arguments or Invalid type arguments detected ..")

      val seeds = Seq(21L, 23L, 25L)
      processCrawled(seeds)

    }

    ssc.start()
    ssc.awaitTermination()

  }
}
