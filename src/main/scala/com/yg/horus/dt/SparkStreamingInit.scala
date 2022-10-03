package com.yg.horus.dt

import com.yg.horus.RuntimeConfig
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

abstract class SparkStreamingInit(sparkAppName: String) {
//  val runtimeConf = RuntimeConfig.getRuntimeConfig()
  val conf = new SparkConf().setMaster(RuntimeConfig("spark.master")).setAppName(sparkAppName)
  val ssc = new StreamingContext(conf, Seconds(10))
}

trait SparkStreamingApp {
  val sparkAppName : String
  //  val runtimeConf = RuntimeConfig.getRuntimeConfig()
  val conf = new SparkConf().setMaster(RuntimeConfig("spark.master")).setAppName(sparkAppName)
  val ssc = new StreamingContext(conf, Seconds(10))
  val spark = SparkSession.builder().config(conf).getOrCreate()
}