package com.yg.horus.dt.termcount

import com.yg.horus.RuntimeConfig
import com.yg.horus.conn.InfluxClient
import com.yg.horus.dt.SparkStreamingInit
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran

import scala.collection.JavaConverters._

class HangleTokenizer extends Serializable {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
//  komoran.setUserDic("./myDic.txt")
//  komoran.setUserDic(getClass.getClassLoader.getResource("myDic.txt").getPath);
  komoran.setUserDic(RuntimeConfig.getRuntimeConfig().getString("komoran.dic"))

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
  override val sparkAppName: String = "SparkStreaming_CrawledTermCount"
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
    if(args.length > 0) {
      System.setProperty("active.profile", args(0))
    } else {
      println("NoArgs .. set on localDevEnv")
      System.setProperty("active.profile", "office_local")
    }

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig.getRuntimeConfig().getString("profile.name"))
    println("------------------------------------------------")

    processCrawled(1L)

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
