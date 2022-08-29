package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.{SparkJobInit, SparkStreamingInit}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, lit, stddev, typedLit, udf, variance}

import java.util.Properties

class TdmMaker(val spark: SparkSession, val model: Word2VecModel) {
  import spark.implicits._

  val revertDouble: UserDefinedFunction = udf((v: Double) => 1 - v)

  val getTermDistance = udf{ (rvsim: Double, vari: Double) =>
    val distPow = math.pow(rvsim, 2)
    math.exp(-1 * distPow / (10 * math.sqrt(vari)))
  }

  def highTermDistances(topicWord: String, limit: Int = 200) = {

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

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "18651865")

    dfWithTs.write.mode(SaveMode.Append).jdbc(RuntimeConfig("spark.jobs.tdm.writeDB"),
      "TERM_DIST", prop)
  }

}

object TdmMaker extends SparkJobInit("TDM") {

  def test(): Unit = {
    val model = Word2VecModel.load("data/w2vNews2Cont_v200_m8_w7_it8")

    val tt = new TdmMaker(spark, model)
    tt.highTermDistances("김") show
  }

  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("W2vTopicCls")
//      .master("local")
//      .getOrCreate()
    test

  }


}
