package com.yg.horus.dt.tdm

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.jblas.{DoubleMatrix => DM}
import org.apache.spark.ml.feature.Word2VecModel

trait W2vDistUtil {
  val spark: SparkSession
  val model: Word2VecModel
  import spark.implicits._

  val df_vectors = model.getVectors.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)

  def word2DM(word: String) = {
    new DM(df_vectors.filter($"word" === word).head.getAs[Vector]("vector").toArray)
  }

  def nearTermsOnVector(srcTerms: Seq[String], limit: Int) = {
    val sumVector = srcTerms.foldLeft(word2DM(srcTerms(0)))((a, b) => a.add(word2DM(b)))
    val centerVector = sumVector.div(srcTerms.length)

    model.findSynonyms(Vectors.dense(centerVector.toArray), limit)
  }

  def nearTermsOnVectorIn(srcTerms: Seq[String], limit: Int) = {
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

  def strNearTermsOnVectorIn(mergedTerms: String, limit: Int) = {
    nearTermsOnVectorIn(mergedTerms.split("\\|").toSeq, limit)
      .orderBy($"similarity".desc)
      .map(_.getAs[String]("word")).collect().mkString("|")
  }
}
