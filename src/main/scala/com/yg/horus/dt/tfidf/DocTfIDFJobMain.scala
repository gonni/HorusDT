package com.yg.horus.dt.tfidf

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DocTfIDFJobMain {
  case class TfidfParam(appName: String = "TF_IDF",
                        master: String = RuntimeConfig("spark.master"),
                        seedId: Long = 1L,
                        limit: Int = 500)

  def main(v: Array[String]): Unit = {
    println("--------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("--------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())

    val runParams = v.length match {
      case 4 => TfidfParam(v(0), v(1), v(2).toLong, v(3).toInt)
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
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new TfIdfProcessing(spark)
    val rawData = test.getRawDataToAnalyze(runParams.seedId, runParams.limit)

    val tfidf = test.tfidf(rawData)
//    println("Save data to DB ..")
//    test.write2db(tfidf, 21L, 300, System.currentTimeMillis())
    println("Calc AVG ..")
    test.avgStatistics(tfidf, runParams.seedId, runParams.limit, System.currentTimeMillis)

    println("Finished ..")
  }
}
