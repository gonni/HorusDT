package com.yg.horus.dt.tfidf


import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, countDistinct, desc, explode, length, size, udf}

import java.util.Properties
import scala.jdk.CollectionConverters.asScalaBufferConverter

class TfIdfProcessing(val spark: SparkSession) extends Serializable {
  import spark.implicits._

  val getNounsUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    if(sentence != null) {
      val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
      komoran.analyze(sentence).getNouns.asScala
    } else {
      Seq[String]()
    }
  }

  val calcIdfUdf1 = udf { df: Long => TfIdfProcessing.calcIdf(100L, df) }

  def getRawDataToAnalyze (limit: Int) = {
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "18651865")

    val tableDf = spark.read.jdbc("jdbc:mysql://localhost:3306/horus?" +
      "useUnicode=true&characterEncoding=utf8&useSSL=false",
      "crawl_unit1", prop)

    val sourceData = tableDf.filter($"SEED_NO" === 9 && $"STATUS" === "SUCC")
      .orderBy(desc("CRAWL_NO"))
      .select($"CRAWL_NO", $"ANCHOR_TEXT", $"PAGE_TEXT")
      .withColumn("document", getNounsUdf($"PAGE_TEXT"))
      .withColumn("token_size", size(col("document"))).limit(limit)

    sourceData
  }


  def tfidf(source: DataFrame) = {
    val documents = source.select($"document", $"CRAWL_NO" as "doc_id")

    val columns = documents.columns.map(col) :+ (explode(col("document")) as "token")
    val unfoldedDocs = documents.select(columns: _*)
    //    unfoldedDocs.show

    val tokenWithTf = unfoldedDocs.groupBy("doc_id", "token").agg(count("document") as "tf")

    val tokenWithDf = unfoldedDocs.groupBy("token").agg(countDistinct("doc_id") as "df")
//    println("=>" + documents.count())
    val tokenWithIdf = tokenWithDf.withColumn("idf", calcIdfUdf1(col("df")))

    val tfidf = tokenWithTf.join(tokenWithIdf, Seq("token"), "left").withColumn("tf_id", col("tf") * col("idf"))
    tfidf
  }

  def write2db(tfdif: DataFrame) = {
    //TODO ...
  }

}

object TfIdfProcessing {
  def calcIdf(docCount: Long, df: Long): Double =
    math.log((docCount.toDouble + 1) / (df.toDouble + 1))
}