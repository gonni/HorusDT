package com.yg.horus.dt.textrank

import com.yg.horus.dt.SparkJobInit
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object TextrankJobMain extends SparkJobInit("DT_TEXT_RANK_JOB"){
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    println("Active System ..")
  }
}
