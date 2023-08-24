package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, col, lit, stddev, typedLit, udf, variance}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.jblas.{DoubleMatrix => DM}

import java.util.Properties

//class TdmMaker(val spark: SparkSession, val model: Word2VecModel) extends W2vDistUtil {
class TdmMaker(val spark: SparkSession) {
  import spark.implicits._

  val revertDouble: UserDefinedFunction = udf((v: Double) => 1 - v)

  val prop = new Properties()
  prop.put("user", RuntimeConfig("mysql.user"))
  prop.put("password", RuntimeConfig("mysql.password"))

  val getTermDistance = udf{ (rvsim: Double, vari: Double) =>
    val distPow = math.pow(rvsim, 2)
    math.exp(-1 * distPow / (10 * math.sqrt(vari)))
  }

  def highTermDistances(model: Word2VecModel, topicWord: String, limit: Int = 20) = {

    val res = model.findSynonyms(topicWord, limit)
    val exRes = res.withColumn("rvsim", revertDouble($"similarity"))
      .withColumn("base_term", typedLit(topicWord))
    val v : Double = exRes.select(variance($"rvsim")).first().getAs[Double](0).toDouble

    val allDf = exRes.withColumn("dist", getTermDistance($"rvsim", lit(v)))

    val dbAll = allDf.withColumnRenamed("base_term", "BASE_TERM")
      .withColumnRenamed("word", "COMP_TERM")
      .withColumnRenamed("dist", "DIST_VAL")
//      .withColumn("GRP_TS", typedLit(ts))

    val dbAll2 = dbAll.select("BASE_TERM", "COMP_TERM", "DIST_VAL")
    dbAll2
  }

  def saveToDB(df: DataFrame, seedNo: Long, tRangeMinAgo:Int, ts: Long): Unit = {
    println("Write Data to DB.Table ------------------------")

    val dfWithTs = df.withColumn("T_RANGE_MIN_AGO", typedLit(tRangeMinAgo))
      .withColumn("SEED_NO", typedLit(seedNo))
      .withColumn("GRP_TS", typedLit(ts))

    dfWithTs.write.mode(SaveMode.Append).jdbc(RuntimeConfig("spark.jobs.tdm.writeDB"),
      "TERM_DIST", prop)
  }

  def saveToTable(df: DataFrame, seedNo: Long, tRangeMinAgo: Int, ts: Long, tableName: String = "dt_common_term_dist"): Unit = {
    println("Write Data to DB.Table ------------------------")

    val dfWithTs = df.withColumn("T_RANGE_MIN_AGO", typedLit(tRangeMinAgo))
      .withColumn("SEED_NO", typedLit(seedNo))
      .withColumn("GRP_TS", typedLit(ts))

    dfWithTs.write.mode(SaveMode.Append).jdbc(RuntimeConfig("spark.jobs.tdm.writeDB"),
      tableName, prop)
  }

  //DF.schema = "BASE_TERM", "NEAR_TERM", "TOPIC_SCORE", "SEED_NO", "GRP_TS"
  def saveMergedTopicTdm(df: DataFrame) = {
    println("Write Data to DB.Table ------------------------")

//    val prop = new Properties()
//    prop.put("user", RuntimeConfig("mysql.user"))
//    prop.put("password", RuntimeConfig("mysql.password"))

    df.write.mode(SaveMode.Append).jdbc(RuntimeConfig("spark.jobs.tdm.writeDB"),
      "DT_TOPIC_TDM", prop)
  }

  def nearTermsOnVectorIn(model: Word2VecModel, srcTerms: Seq[String], limit: Int) = {
    val df_vectors = model.getVectors.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)

    def word2DM(word: String) = {
      new DM(df_vectors.filter($"word" === word).head.getAs[Vector]("vector").toArray)
    }

    val filtered = srcTerms.filter(term => {
      df_vectors.filter($"word" === term).count() > 0
    })
    //    println("Filtered Size = " + filtered.size)

    val sumVector = filtered.foldLeft(word2DM(filtered(0)))((a, b) => a.add(word2DM(b)))
    val centerVector = sumVector.div(filtered.length)

    val terms = model.findSynonyms(Vectors.dense(centerVector.toArray), limit + filtered.size)
    terms.filter(term => {
      !filtered.contains(term.getAs[String]("word"))
    })
  }

  def strNearTermsOnVectorIn(model: Word2VecModel, mergedTerms: String, limit: Int) = {
    nearTermsOnVectorIn(model, mergedTerms.split("\\|").toSeq, limit)
      .orderBy($"similarity".desc)
      .map(_.getAs[String]("word")).collect().mkString("|")
  }

}

object TdmMaker {

//  def test(): Unit = {
//    val model = Word2VecModel.load("data/w2vNews2Cont_v200_m8_w7_it8")
//
//    val tt = new TdmMaker(spark, model)
//    tt.highTermDistances("김").show()
//    tt.highTermDistances(Seq("A"))
//  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("W2vTopicCls")
      .master("local")
      .getOrCreate()
//    test

    val model = Word2VecModel.load("data/w2vNews2Cont_v200_m8_w7_it8")

//    val tt = new TdmMaker(spark, model)
////    tt.highTermDistances("김").show()
//
////    tt.highVectorDistances(List("한국", "러시아", "우크라이나"))
//
//    val nearTerms = tt.nearTermsOnVector(Seq("한국", "러시아", "우크라이나"), 10)
//    nearTerms.toDF().show()
  }


}
