package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object W2vMain {
  def main(args: Array[String]): Unit = {

    println("-------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("-------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())

    val conf = new SparkConf().setMaster(RuntimeConfig("spark.master")).setAppName("W2V_Modeling")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new Word2vecModeler(spark)
    //    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(3))
    //    test.loadSource(25L, fromTime) show
    //    println("count of data =>" + test.loadSourceFromDaysAgo(21L, 20).count())

    val data = test.loadSourceFromMinsAgo(1L, 60)
    //    data.show()
    val model = test.createModel(data)
    println("Result ..")
    model.findSynonyms("김", 30).show(100)

    //    test.saveModelToFile(model, "/usr/local/spark/resources/horus/w2v_d1_2022082112")
    test.saveModelToFile(model, RuntimeConfig("spark.jobs.word2vec.modelFile") + "w2v_common_" + System.currentTimeMillis())
    println("fit completed ..")

  }
}
