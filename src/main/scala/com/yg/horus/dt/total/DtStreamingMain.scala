package com.yg.horus.dt.total

import com.yg.horus.dt.termcount.MySqlSourceReceiver
import com.yg.horus.dt.topic.LdaTopicProcessing
import com.yg.horus.dt.{SparkStreamingApp, SparkStreamingInit}

object DtStreamingMain extends SparkStreamingApp {
  override val sparkAppName: String = "HORUS_DT_STREAM"

  // LDA -> W2V -> TFIDF
  def main(args: Array[String]): Unit = {
    val seedId = 21
    val bodyText = ssc.receiverStream(new MySqlSourceReceiver(seedId))
    //TODO Logic Implementation



    val test = new LdaTopicProcessing(spark)





    ssc.start()
    ssc.awaitTermination()
  }
}
