package com.yg.horus.dt.termcount

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]): Unit = {
    println("Active System ..")
    if(args.length > 0) {
      println("Input detected profile :", args(0))
      System.setProperty("active.profile", args(0))
    } else {
      println("No Args .. set on " + RuntimeConfig.getActiveProfile())
      System.setProperty("active.profile", RuntimeConfig.getActiveProfile())
    }

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig.getRuntimeConfig().getString("profile.name"))
    println("------------------------------------------------")


    val runtimeConf = RuntimeConfig.getRuntimeConfig()
    val conf = new SparkConf().setMaster(runtimeConf.getString("spark.master")).setAppName("DEP_TEST_APP")
    val ssc = new StreamingContext(conf, Seconds(10))

    val jobProc = new TermCountProcessing(ssc)
    jobProc.processCrawled(21L)

    ssc.start()
    ssc.awaitTermination()
  }
}
