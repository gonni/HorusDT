package com.yg.horus.dt.total

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.topic.LdaTopicProcessing
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.{DoubleMatrix => DM}

import java.sql.Timestamp
import java.time.LocalDateTime
class RealtimeTopic(val spark: SparkSession) {
  import spark.implicits._

  val model = Word2VecModel.load("data/w2v_2m")
  val df_vectors = model.getVectors.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)

  println("Loaded Model =======================")
  df_vectors.show()

  def word2DM(word: String) = {
    new DM(df_vectors.filter($"word" === word).head.getAs[Vector]("vector").toArray)
  }

  def nearTermsOnVector(srcTerms: Seq[String], limit: Int) = {
    //    val sumVector = srcTerms.reduce((a1, a2) => word2DM(a1).add(word2DM(a2)))
    val sumVector = srcTerms.foldLeft(word2DM(srcTerms(0)))((a, b) => a.add(word2DM(b)))
    val centerVector = sumVector.div(srcTerms.length)

    model.findSynonyms(Vectors.dense(centerVector.toArray), limit)
  }

  def anaylzeTopicTerms() = {
    ;
  }
}

object RealtimeTopic {
  case class RunParams(appName: String, master: String, seedNo: Long, minAgo: Int,
                       cntTopic: Int = 10, cntTopicTerms: Int = 10)

  def main(args: Array[String]): Unit = {
    println("--------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("--------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())

    val rtParam = args.length match {
      case 6 => RunParams(args(0), args(1), args(2).toLong, args(3).toInt, args(4).toInt, args(5).toInt)
      case _ => RunParams("LDA_TOPIC_TERMS", RuntimeConfig("spark.master"), 21L, 600)
    }

    println(s"Applied Params : ${rtParam}")

    val conf = new SparkConf().setMaster(rtParam.master).setAppName(rtParam.appName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new LdaTopicProcessing(spark)
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(rtParam.minAgo))
    val source = test.loadSource(rtParam.seedNo, fromTime)

    source.show()

    val w2v = new RealtimeTopic(spark)

    println("----- Topic terms -----")
    val topics = test.topics(source, 10, 15)
    val fRes = test.convertObject(topics)

    for (i <- 0 until fRes.length) {
      println(s"Topic #${i}")
      fRes(i).foreach(a => println(a))

      println("------------ Near Terms -------------")
      w2v.nearTermsOnVector(fRes(i).map(_.term), 20).show(20)


      println("======================")
    }

//    test.saveToDB(topics, rtParam.seedNo, rtParam.minAgo)

    spark.close()
  }
}