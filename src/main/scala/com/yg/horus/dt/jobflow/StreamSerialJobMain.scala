package com.yg.horus.dt.jobflow


import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamSerialJobMain {

  def processStream(implicit ssc: StreamingContext) = {
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

    val runtimeConf = RuntimeConfig.getRuntimeConfig()
    val conf = new SparkConf().setMaster(runtimeConf.getString("spark.master")).setAppName("STREAM-MULTI-SEEDS-TC")
    //  val spark = SparkSession.builder().config(conf).getOrCreate()
    implicit val ssc = new StreamingContext(conf, Seconds(10))
    processStream

    ssc.start()
    ssc.awaitTermination()

  }
}
