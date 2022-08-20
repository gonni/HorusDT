package com.yg.horus.dt

import com.yg.horus.RuntimeConfig
import org.apache.spark._
import org.apache.spark.sql._

abstract class SparkJobInit (sparkAppName: String) {
  val runtimeConf = RuntimeConfig.getRuntimeConfig()
  val conf = new SparkConf().setMaster(runtimeConf.getString("spark.master")).setAppName(sparkAppName)
  val spark = SparkSession.builder().config(conf).getOrCreate()
}
