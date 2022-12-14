package com.yg.horus.dt.termcount

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

@deprecated
object Main {
  def main(args: Array[String]): Unit = {
    println("Active System ..")

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig("profile.name"))
    println("------------------------------------------------")
    println("Runtime Config => " + RuntimeConfig())

    val runtimeConf = RuntimeConfig.getRuntimeConfig()
    val conf = new SparkConf()
      .setMaster(runtimeConf.getString("spark.master"))
      .setAppName("TERM-COUNT-STREAM")
    val ssc = new StreamingContext(conf, Seconds(10))

    val jobProc = new TermCountProcessing(ssc)
    jobProc.processCrawled(21L)

    ssc.start()
    ssc.awaitTermination()
  }
}
