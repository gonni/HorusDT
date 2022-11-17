package com.yg.horus.dt.jobflow

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.termcount.DbUtil
import com.yg.horus.dt.topic.LdaTopicProcessing
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import java.sql.Timestamp
import java.time.LocalDateTime


object StreamSerialJobMain {



  def main(args: Array[String]): Unit = {
    println("Active System..")

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig("profile.name"))
    println("------------------------------------------------")
    println("RuntimeConfig Details : " + RuntimeConfig())

    val conf = new SparkConf().setMaster(RuntimeConfig("spark.master")).setAppName("SC-AND-SSC")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(ss.sparkContext, Seconds(10))

    import ss.implicits._

    val res = ssc.receiverStream(new MySqlDataPointReceiver(1L))

    res.foreachRDD((rdd, time) => {
      rdd.foreach(println)
      val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(600))

//      val lda = new LdaTopicProcessing(ss)
//      lda.loadSource(1L, fromTime).foreach(row => println(row.mkString("|")))
//      val job =  LdaTdmJoblet(spark, 21, 60, 60 k)(1L)
      println(s"Finished Processing Term ------------- ${time}")
    })

    ssc.start()
    ssc.awaitTermination()
    ss.close()
  }
}

class MySqlDataPointReceiver(val seedNo : Long) extends Receiver[Long](StorageLevel.MEMORY_AND_DISK_2)
  with Logging {

  var latestCrawlNo = 0L

  override def onStart(): Unit = {
    new Thread("MysqlSt") {
      override def run(): Unit = {
        createGetData
      }
    }.start()
  }

  override def onStop(): Unit = synchronized {
    //    this.db.close()
  }

  private def createGetData(): Unit = {
    while(!isStopped) {
      try {
        latestCrawlNo = DbUtil.latestCrawlNo(seedNo)
        store(latestCrawlNo)

        println(s"Update Point ${latestCrawlNo} for seed#${seedNo}")

        Thread.sleep(5000)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}