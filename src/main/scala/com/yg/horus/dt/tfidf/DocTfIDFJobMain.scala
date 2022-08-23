package com.yg.horus.dt.tfidf

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DocTfIDFJobMain {
  case class TfidfParam(appName: String, master: String, seedId: Long, limit: Int)

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    println("--------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("--------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())

    val runParams = args.length match {
      case _ => TfidfParam("TFIDF_JOB", "local", 9, 100)
    }

    val conf = new SparkConf().setMaster(runParams.master).setAppName(runParams.appName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new TfIdfProcessing(spark)
    val rawData = test.getRawDataToAnalyze(100)
    test.tfidf(rawData) show

    println("Finished ..")
  }
}
