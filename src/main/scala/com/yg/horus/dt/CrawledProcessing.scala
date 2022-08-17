package com.yg.horus.dt

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran

import scala.collection.JavaConverters._

class HangleTokenizer extends Serializable {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
  komoran.setUserDic("./myDic.txt")
  def arrayTokens(sentence : String) = {
    val tokens = komoran.analyze(sentence).getTokenList.asScala.map(_.getMorph)
    tokens
  }

  def arrayNouns(sentence: String) = {
    komoran.analyze(sentence).getNouns.asScala
  }
}

object HangleTokenizer {
  def apply() : HangleTokenizer = new HangleTokenizer
}

object CrawledProcessing extends SparkStreamingInit {

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

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    processCrawled(9L)

//    val anchors = ssc.receiverStream(new MySqlSourceReceiver(Seq(9L)))
//    val words = anchors.flatMap(anchor => {
////      HangleTokenizer().arrayTokens(anchor)
//      HangleTokenizer().arrayNouns(anchor)
//    })
//
////    val words = anchors.flatMap(_.split(" "))
//    // Count each word in each batch
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//
//    // Print the first ten elements of each RDD generated in this DStream to the console
//    wordCounts.print
////    wordCounts.foreachRDD((a, b) => {println(a + "->" + b)})
//    wordCounts.foreachRDD(rdd => {
//      rdd.foreach(tf => {
////        println(tf._1 + " --> " + tf._2)
//        InfluxClient.writeTf(1L, tf._1, tf._2)
//      })
//    })

    ssc.start()
    ssc.awaitTermination()
  }

}
