package com.yg.horus.dt.tfidf

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DocTfIDFJobMain {
  case class TfidfParam(appName: String = "TF_IDF",
                        master: String = "local[*]",
                        seedId: Long = 9L,
                        limit: Int = 100)

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
    val rawData = test.getRawDataToAnalyze(21L, 5000)

    val tfidf = test.tfidf(rawData)
    println("Save data to DB ..")

    test.write2db(tfidf, 21L, 300, System.currentTimeMillis())

    println("Finished ..")
  }
}
