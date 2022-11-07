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
  komoran.setUserDic(RuntimeConfig("komoran.dic"))

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

object CrawledProcessingMain extends SparkStreamingInit("SPP") {
//  override val sparkAppName: String = "SparkStreaming_CrawledTermCount"
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

  def processCrawleds(seedIds: Seq[Long]) = {
    seedIds.foreach(seedId => {
      processCrawled(seedId)
    }
    )
  }


  def main(v: Array[String]): Unit = {
    println("Active System ..")

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig.getRuntimeConfig().getString("profile.name"))
    println("------------------------------------------------")
    println("RuntimeConfig Details : " + RuntimeConfig())

    if(v.length > 0) {
      println("Count of Input Arguments => " + v.length)
      val seeds = v.map(args => {
        args.toLong
      }).toSeq

      processCrawleds(seeds)

      ssc.start()
    } else {
      println("No input arguments or Invalid type arguments detected ..")
    }

//    processCrawled(1L)

    ssc.awaitTermination()
  }


}
