package com.yg.horus.dt.jobflow

import com.yg.horus.RuntimeConfig
import org.apache.spark._
import org.apache.spark.sql._

object ExSerialJobMain {
  implicit def k(v: Int): Kilo = new Kilo(v)

  class Kilo(val x: Int) {
    def k(): Long = x * 1000L
  }

  def main(args: Array[String]): Unit = {
    println("Active System ..")


    for(i <- 1 to 20000) {
      println("Craete New Spark Session .. turn#" + i)
      val sc = createNewSparkSession()

      val logger = new DtLogger(sc)
      logger.logJob("START_SC_JOB_TURN_"+i, "SUC")

      val jobManager = new SeiralJobManager(cntTurns = 3, checkPeriod = 5000L)
      jobManager.addJob(new LdaTdmJoblet(sc, 21, 60, 300 k))
      jobManager.addJob(new LdaTdmJoblet(sc, 25, 60, 600 k))
      jobManager.addJob(new LdaTdmJoblet(sc, 23, 60, 1200 k))
      //      jobManager.addJob(new LdaTdmJoblet(spark, 1, 60, 120 k))
      //      jobManager.addJob(new LdaTdmJoblet(spark, 2, 60, 300 k))

      jobManager.start()

      println(s"----------------------- Close Spark Session : ${i}----------------------")
      logger.logJob("FINISH_SC_JOB_TURN_"+i, "SUC")
      sc.close()
    }
    println("Job Completed")
  }

  def createNewSparkSession() = {
    val runtimeConf = RuntimeConfig.getRuntimeConfig()
    val conf = new SparkConf()
      .setMaster(runtimeConf.getString("spark.master"))
      .setAppName("EX_SERIAL_JOB_" + System.currentTimeMillis())
    SparkSession.builder().config(conf).getOrCreate()
  }
}
