package com.yg.horus.dt.termcount

import com.yg.horus.conn.InfluxClient
import org.apache.spark.streaming.StreamingContext

@deprecated
class TermCountProcessing(val ssc : StreamingContext) {

  def processCrawled(seedId : Long) = {
    val anchors = ssc.receiverStream(new MySqlSourceReceiver(seedId))
    val words = anchors.flatMap(anchor => {
      HangleTokenizer().arrayNouns(anchor)
    })

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print

    wordCounts.foreachRDD(rdd => {
      rdd.foreach(tf => {
        InfluxClient.writeTf(seedId, tf._1, tf._2)
      })
    })

  }
}
