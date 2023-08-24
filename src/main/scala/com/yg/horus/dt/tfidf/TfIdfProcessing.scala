package com.yg.horus.dt.tfidf

import com.yg.horus.RuntimeConfig
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, desc, explode, length, size, typedLit, udf}

import java.util.Properties
import scala.jdk.CollectionConverters.asScalaBufferConverter

class TfIdfProcessing(val spark: SparkSession) extends Serializable {
  import spark.implicits._

  val getNounsUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    if(sentence != null) {
      val komoran = new Komoran(DEFAULT_MODEL.FULL)
      komoran.analyze(sentence).getNouns.asScala
    } else {
      Seq[String]()
    }
  }

  val calcIdfUdf1 = udf { df: Long => TfIdfProcessing.calcIdf(100L, df) }

//  val calcIdfUdf2 = udf { (df: Long, docCount: Long) => TfIdfProcessing.calcIdf(docCount, df) }

  def getRawDataToAnalyze (seedNo: Long, limit: Int) = {
    println("processing GET_DATA_FROM_DB .................. ")
    val prop = new Properties()
    prop.put("user", RuntimeConfig("mysql.user"))
    prop.put("password", RuntimeConfig("mysql.password"))

    val tableDf = spark.read.jdbc(RuntimeConfig("mysql.url"), "crawl_unit1", prop)

    val sourceData = tableDf.filter($"SEED_NO" === seedNo && $"STATUS" === "SUCC")
//      .orderBy(desc("CRAWL_NO"))
      .select($"CRAWL_NO", $"ANCHOR_TEXT", $"PAGE_TEXT")
      .withColumn("document", getNounsUdf($"PAGE_TEXT"))
      .withColumn("token_size", size(col("document"))).limit(limit)

    val fd = sourceData.filter($"token_size" > 0)
    fd
  }

  def tfidf(source: DataFrame) = {
    println("processing TFIDF .................. ")

    val documents = source.select($"document", $"CRAWL_NO" as "doc_id")

    val columns = documents.columns.map(col) :+ (explode(col("document")) as "token")
    val unfoldedDocs = documents.select(columns: _*)
//    unfoldedDocs.show(10)

    val tokenWithTf = unfoldedDocs.groupBy("doc_id", "token").agg(count("document") as "tf")

    val tokenWithDf = unfoldedDocs.groupBy("token").agg(countDistinct("doc_id") as "df")

    println("start re-partition ......")

    val docCount = 5000L //documents.count()
    println("re-partition finished......")

    val calcIdfUdf2 = udf { df: Long => TfIdfProcessing.calcIdf(docCount, df) }
//    val tokenWithIdf = tokenWithDf.withColumn("idf", calcIdfUdf1(col("df")))
    val tokenWithIdf = tokenWithDf.withColumn("idf", calcIdfUdf2(col("df")))

    val tfidf = tokenWithTf.join(tokenWithIdf, Seq("token"), "left")
      .withColumn("tfidf", col("tf") * col("idf"))
//    tfidf.show()
    tfidf
  }

  def write2db(tfidf: DataFrame, seedNo: Long, minAgo: Int, grpTs: Long) = {
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

    val exTfidf = tfidf
      .withColumn("START_MIN_AGO", typedLit(minAgo))
      .withColumn("SEED_NO", typedLit(seedNo))
      .withColumn("GRP_TS", typedLit(grpTs))

    println("save ifidf data to db")
    exTfidf show

    exTfidf.write.mode(SaveMode.Append).jdbc(RuntimeConfig("mysql.url"), "DT_TFIDF", prop)
  }

  // tfidf = TFIDF_NO | TOKEN | TF | DF | IDF | TFIDF | START_MIN_AGO | SEED_NO | GRP_TS
  def avgStatistics(tfidf: DataFrame, seedNo: Long, dataRangeMin: Int, grpTs: Long): Unit = {
    println("Processing data to store db ........")

    val tableData = tfidf.groupBy("TOKEN").agg(
      avg("tfidf").as("AVG_TFIDF"),
      avg("df").as("AVG_DF"))
      .withColumn("DATA_RANGE_MIN", typedLit(dataRangeMin))
      .withColumn("SEED_NO", typedLit(seedNo))
      .withColumn("GRP_TS", typedLit(grpTs))

//    tableData.show(10)

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

    tableData.write.mode(SaveMode.Append).jdbc(RuntimeConfig("mysql.url"), "DT_TERM_SCORE", prop)
  }

}

object TfIdfProcessing {
  def calcIdf(docCount: Long, df: Long): Double =
    math.log((docCount.toDouble + 1) / (df.toDouble + 1))
}