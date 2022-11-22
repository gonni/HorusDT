package com.yg.horus.dt.util

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.topic.LdaTopicProcessing
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties
import scala.io.Source
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object Hangle {
  //extends SparkJobInit("HANGLE_PARSE_TEST") {

  def main(v: Array[String]): Unit = {
    if(v.length == 2) {
      println(s"No Params : v[0]:profile = ${v(0)}, v[1]:dicLocation = ${v(1)}" )
      RuntimeConfig.setEnv(v(0))
    }

    println("Active Config =>" + RuntimeConfig())

    val runtimeConf = RuntimeConfig.getRuntimeConfig()
    val conf = new SparkConf()
      .setMaster(runtimeConf.getString("spark.master"))
      .setAppName("TEST_HANGLE_" + System.currentTimeMillis())
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

    println("--- File Path -> " + new File(".").getAbsolutePath)
    println("--- Res Path -> " + Hangle.getClass.getClassLoader.getResource(".").toURI.getPath)

    var dicLocation = RuntimeConfig("komoran.dic")
    var src = "대통령 윤석렬은 유지할 수 있을까? 알코르에서 대통령실 영부인 김건희는 사기꾼이 확실한데 .. 윤 대통령을 국민의 힘 국민의힘이 지켜줄라나"
    var minAgo = 600

    Source.fromFile(dicLocation).getLines().foreach(line => {
      println("=>" + line.split("\t").toSeq)
    })

    komoran.setUserDic(dicLocation)

    println("----------- Result parsed -------------")
    komoran.analyze(src).getNouns.forEach(println)


    println("----------- Result parsed to DF -------------")
    val df = komoran.analyze(src).getNouns.asScala.toDF.select($"value" as "res")
    df.show()

    val prop = new Properties()
    prop.put("user", RuntimeConfig("mysql.user"))
    prop.put("password", RuntimeConfig("mysql.password"))

    df.write.mode(SaveMode.Append).jdbc(RuntimeConfig("spark.jobs.lda.writeDB"),
      "DT_TEST_TEMP", prop)

    println("=======================================")
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(minAgo))
    val lda = new LdaTopicProcessing(spark)
    val source = lda.loadSource(1, fromTime)
    println("[Source Data for LDA] ----------------- ")
    source.show(30)
    source.filter(_.getAs[String]("ANCHOR_TEXT").contains("윤석열")).foreach(row => {
      println(row.mkString("|"))
    })

    val db = source.select($"PAGE_TEXT", $"tokenized").map(_.mkString).select($"value" as "res")
//    db.select("")
    db.show(30)
    println(db.take(1).mkString("|"))
    db.filter(_.getAs[String]("res").contains("윤석열")).foreach(row => {
      println(row.mkString("**"))
    })


    db.write.mode(SaveMode.Append)
    .jdbc(RuntimeConfig("spark.jobs.lda.writeDB"),
    "DT_TEST_TEMP", prop)

//
////    println("=======================================")
////    source.foreach(row => {
////      val a = row.mkString("|")
////      if(a.contains("윤석") || a.contains("알코르,") || a.contains("국민의"))
////        println(a)
////    })


    spark.close()
  }
}
