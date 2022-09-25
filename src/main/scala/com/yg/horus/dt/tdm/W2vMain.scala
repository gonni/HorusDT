package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object W2vMain {
  case class RunArgs(appName: String = "W2V_Model_File_Creation",
                     master: String = RuntimeConfig("spark.master"),
                     seedNo: Long = 1L,
                     minAgo: Int = 7000,
                     modelFilePath: String = RuntimeConfig("spark.jobs.word2vec.modelFile") + "/w2v_d101_" + System.currentTimeMillis)

  def main(v: Array[String]): Unit = {
    var runParam = v.length match {
      case 2 => RunArgs(seedNo = v(0).toLong, minAgo = v(1).toInt)
      case 3 => RunArgs(seedNo = v(0).toLong, minAgo = v(1).toInt, modelFilePath = v(2))
      case _ => {
        println("Custom Usage with 2 args: [SeedNO] [fromMinAgo]")
        println("Custom Usage with 3 args: [SeedNO] [fromMinAgo] [modelFilePath]")
        RunArgs()
      }
    }
    println("-------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("-------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())
    println("-------------------------------------")
    println("Input Params : " + runParam)


    val conf = new SparkConf().setMaster(runParam.master).setAppName(runParam.appName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new Word2vecModeler(spark)
    //    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(3))
    //    test.loadSource(25L, fromTime) show
    //    println("count of data =>" + test.loadSourceFromDaysAgo(21L, 20).count())

    val data = test.loadSourceFromMinsAgo(runParam.seedNo, runParam.minAgo)
    //    data.show()
    val model = test.createModel(data)
    println("Result ..")
    model.findSynonyms("김", 30).show(100)

    println("Write file to " + runParam.modelFilePath)
    //    test.saveModelToFile(model, "/usr/local/spark/resources/horus/w2v_d1_2022082112")
    test.saveModelToFile(model, runParam.modelFilePath)
    println("fit completed ..")

  }
}
