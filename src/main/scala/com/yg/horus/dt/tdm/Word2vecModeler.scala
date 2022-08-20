package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.SparkJobInit
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, desc, udf}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties
import scala.jdk.CollectionConverters.asScalaBufferConverter

class Word2vecModeler(val spark: SparkSession) {
  import spark.implicits._

//  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
//  komoran.setUserDic(RuntimeConfig.getRuntimeConfig().getString("komoran.dic"))

  // source schema must be like : ANCHOR_TEST | PAGE_TEXT | tokenized


  def createModel(df : DataFrame) = {
    Word2vecModeler.createW2vModel(df)
  }

  def saveModelToFile(model : Word2VecModel, filePath: String) = {
    model.save(filePath)
  }

  def loadSourceFromMinsAgo(seedNo: Long, minAgo: Int) = {
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(minAgo))
    loadSource(seedNo, fromTime)
  }

  def loadSourceFromDaysAgo(seedNo: Long, minAgo: Int) = {
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusDays(minAgo))
    loadSource(seedNo, fromTime)
  }

  def loadSource(seedNo: Long, fromTime: java.sql.Timestamp) = {


    val getTokenListUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
      try {
        Word2vecModeler.komoran.analyze(sentence).getTokenList.asScala.map(_.getMorph)
      } catch {
        case e: Exception => {
          println("Detected Null Pointer .. " + e.getMessage)
          Seq()
        }
      }
    }

    val prop = new Properties()
    prop.put("user", RuntimeConfig().getString("mysql.user"))
    prop.put("password", RuntimeConfig().getString("mysql.password"))

    val tableDf = spark.read.jdbc(RuntimeConfig().getString("mysql.url"), "crawl_unit1", prop)
    tableDf.filter($"SEED_NO" === seedNo && $"REG_DATE" > fromTime)
      .orderBy(desc("CRAWL_NO"))
      .select($"ANCHOR_TEXT", $"PAGE_TEXT")
      .withColumn("tokenized", getTokenListUdf2($"PAGE_TEXT"))
  }

}

object Word2vecModeler extends SparkJobInit("W2VM") {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
  komoran.setUserDic(RuntimeConfig.getRuntimeConfig().getString("komoran.dic"))

  def createW2vModel(df: DataFrame) = {

    new Word2Vec()
      .setInputCol("tokenized")
      .setOutputCol("vector")
      .setVectorSize(200)
      .setMinCount(8)
      .setWindowSize(7)
      .setMaxIter(8).fit(df)

      //      word2Vec.fit(source)
  }

  def main(args: Array[String]): Unit = {
    println("Active ..")
    val test = new Word2vecModeler(spark)
//    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(3))
//    test.loadSource(25L, fromTime) show
//    println("count of data =>" + test.loadSourceFromDaysAgo(21L, 20).count())

    val data = test.loadSourceFromDaysAgo(21L, 5)
//    data.show()
    val model = test.createModel(data)
    println("Result ..")
    model.findSynonyms("김",30).show(100)
  }
}