package com.yg.horus.dt.tfidf

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DocTfIDFJobMain {
  case class TfidfParam(appName: String = "TF_IDF",
                        master: String = "local[*]",
                        seedId: Long = 9L,
                        limit: Int = 100)

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    println("--------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("--------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())

    val runParams = args.length match {
      case 4 => TfidfParam("TFIDF_JOB", "local", 9, 100)
      case _ =>
        if(RuntimeConfig.getActiveProfile().contains("home"))
          TfidfParam(seedId = 21L)
        else
          TfidfParam()
    }
    println("--------------------------------------")
    println(s"TF-IDF Job Args : ${runParams}")
    println("--------------------------------------")

    val conf = new SparkConf().setMaster(runParams.master).setAppName(runParams.appName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new TfIdfProcessing(spark)
    val rawData = test.getRawDataToAnalyze(1L, 100)

    val tfidf = test.tfidf(rawData)
    println("Save data to DB ..")

    test.write2db(tfidf, 1L, 300, System.currentTimeMillis())

    println("Finished ..")
  }
}
