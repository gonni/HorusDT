package com.yg.horus.dt.jobflow

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.jobflow.ExSerialJobMain.createNewSparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RunOnceMain {
  def main(args: Array[String]): Unit = {
    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig("profile.name"))
    println("------------------------------------------------")
    println("RuntimeConfig Details : " + RuntimeConfig())

    println("Arg Length -> " + args.length)

    args.length match {
      case 1 => {
        val seedNo = args(0).toLong
        val sc = createNewSparkSession(seedNo)
        new LdaTdmJoblet(sc, seedNo, 60, 0).run()
      }
      case _ => println("Invalid Parameter ..")
    }
  }

  def createNewSparkSession(seedId: Long) = {
    val runtimeConf = RuntimeConfig.getRuntimeConfig()
    val conf = new SparkConf()
      .setMaster(runtimeConf.getString("spark.master"))
      .setAppName(s"Single_${seedId}_JOB_" + System.currentTimeMillis())
    SparkSession.builder().config(conf).getOrCreate()
  }

}
