package com.yg.horus.dt.tdm

import org.jblas.DoubleMatrix
import org.jblas.{DoubleMatrix => DM}
import breeze.linalg.DenseVector
import breeze.linalg.{DenseVector => DV}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

import com.yg.horus.dt.SparkJobInit
import org.apache.spark.ml.feature.Word2VecModel

class WordVectorAnalyzer {
  // TBD
}

object WordVectorAnalyzer extends SparkJobInit("W2vAnalyzer") {
  import spark.implicits._
  val model = Word2VecModel.load("data/w2v_d101_1663837864518")
  val df_vectors = model.getVectors.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)

  println("=======================")
  df_vectors.show()
  println("=======================")

  def word2DM(word: String) = {
//    new DM(df_vectors.where($"word" === word).head.getAs[Vector]("vector").toArray)
    val a = new DM(df_vectors.filter($"word" === word).head.getAs[Vector]("vector").toArray)
    println(word + "-->" + a)
    a
  }

//  def word2DV(word: String) = {
//    new DV(df_vectors.where('word === word).head.getAs[Vector]("vector").toArray)
//  }

  def searchByRelation(positiveWord: String, positiveWord2: String, num: Int): DataFrame = {
    val target = word2DM(positiveWord).add(word2DM(positiveWord2))
    val result = model.findSynonyms(Vectors.dense(target.toArray), num)
    result
  }

  def searchByRelation(positiveWord: String, negativeWord: String, positiveWord2: String, num: Int): DataFrame = {
    val target = word2DM(positiveWord).sub(word2DM(negativeWord)).add(word2DM(positiveWord2))
    val result = model.findSynonyms(Vectors.dense(target.toArray), num)
    result
  }

  def test(): Unit = {
//    val model = Word2VecModel.load("data/w2vNews2Cont_v200_m8_w7_it8")
////    val tt = new TdmMaker(spark, model)
////    tt.highTermDistances("김") show
//
//    val df_vectors = model.getVectors.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
  }

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    val positive_word_01 = "일본"
//    val negative_word_02 = "대한민국"
    val positive_word_03 = "중국"
    val result = searchByRelation(positive_word_01,  positive_word_03, 10)
    result.show()

  }


}