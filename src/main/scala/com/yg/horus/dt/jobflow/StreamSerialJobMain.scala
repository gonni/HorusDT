package com.yg.horus.dt.jobflow

import com.yg.horus.dt.SparkStreamingInit
import com.yg.horus.RuntimeConfig

object StreamSerialJobMain extends SparkStreamingInit("STREAMING-SERIAL-JOB"){

  def processStream() = {
    val rcvd = ssc.receiverStream(new SerialPeriodMysqlSourceReceiver(600))

    rcvd.print()
    println("Fin -------------------------------------------")
  }

  def main(args: Array[String]): Unit = {
    println("Active System..")

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig("profile.name"))
    println("------------------------------------------------")
    println("RuntimeConfig Details : " + RuntimeConfig())

    processStream()

    ssc.start()
    ssc.awaitTermination()

    spark.close()
  }
}
