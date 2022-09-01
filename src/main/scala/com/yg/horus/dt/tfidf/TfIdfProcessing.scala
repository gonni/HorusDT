package com.yg.horus.dt.tfidf


import com.yg.horus.RuntimeConfig
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, countDistinct, desc, explode, length, size, typedLit, udf}

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

  def getRawDataToAnalyze (seedNo: Long, limit: Int) = {
    val prop = new Properties()
    prop.put("user", RuntimeConfig("mysql.user"))
    prop.put("password", RuntimeConfig("mysql.password"))

    val tableDf = spark.read.jdbc(RuntimeConfig("mysql.url"), "crawl_unit1", prop)

    val sourceData = tableDf.filter($"SEED_NO" === seedNo && $"STATUS" === "SUCC"
      && $"CRAWL_NO" > 210100L && $"CRAWL_NO" < 220320L)
      .orderBy(desc("CRAWL_NO"))
      .select($"CRAWL_NO", $"ANCHOR_TEXT", $"PAGE_TEXT")
      .withColumn("document", getNounsUdf($"PAGE_TEXT"))
      .withColumn("token_size", size(col("document"))).limit(limit)

//    sourceData.show()

    val fd = sourceData.filter($"token_size" > 0)
    fd.show(300)
    fd
//    sourceData
  }

  def tfidf(source: DataFrame) = {
    val documents = source.select($"document", $"CRAWL_NO" as "doc_id")

    val columns = documents.columns.map(col) :+ (explode(col("document")) as "token")
    val unfoldedDocs = documents.select(columns: _*)
    unfoldedDocs.show

    val tokenWithTf = unfoldedDocs.groupBy("doc_id", "token").agg(count("document") as "tf")

//    println("unfoleded ------------------")
//    unfoldedDocs.show(10000)

    val tokenWithDf = unfoldedDocs.groupBy("token").agg(countDistinct("doc_id") as "df")
    //    println("=>" + documents.count())
    val tokenWithIdf = tokenWithDf.withColumn("idf", calcIdfUdf1(col("df")))

    val tfidf = tokenWithTf.join(tokenWithIdf, Seq("token"), "left")
      .withColumn("tfidf", col("tf") * col("idf"))
    tfidf
  }

  def write2db(tfidf: DataFrame, seedNo: Long, minAgo: Int, grpTs: Long) = {
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "18651865")

    tfidf.show()

    val exTfidf = tfidf
      .withColumn("START_MIN_AGO", typedLit(minAgo))
      .withColumn("SEED_NO", typedLit(seedNo))
      .withColumn("GRP_TS", typedLit(grpTs))

    exTfidf show

    exTfidf.write.mode(SaveMode.Append).jdbc(RuntimeConfig("mysql.url"), "DT_TFIDF", prop)
  }

}

object TfIdfProcessing {
  def calcIdf(docCount: Long, df: Long): Double =
    math.log((docCount.toDouble + 1) / (df.toDouble + 1))
}