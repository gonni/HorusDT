package com.yg.horus.dt.topic

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.linalg.{SparseVector}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}
import org.apache.spark.ml.clustering.LDA

object LdaProcessing {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("text lda")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    // wget http://kdd.ics.uci.edu/databases/20newsgroups/mini_newsgroups.tar.gz -O /tmp/newsgroups.tar.gz
    val corpus = sc.wholeTextFiles("/Users/a1000074/dev/temp-news/mini_newsgroups/*")
      .map(_._2).map(_.toLowerCase)

    println("corpus ---- ")
//    corpus.takeSample(false, 1).map(println)

    val corpus_body = corpus.map(_.split("\\n\\n")).map(_.drop(1)).map(_.mkString(" "))
//    corpus_body.take(5).foreach(println)
    val corpus_df = corpus_body.zipWithIndex.toDF("corpus", "id")
//    corpus_df show(10)

    // Set params for RegexTokenizer
    val tokenizer = new RegexTokenizer()
      .setPattern("[\\W_]+")
      .setMinTokenLength(4) // Filter away tokens with length < 4
      .setInputCol("corpus")
      .setOutputCol("tokens")
    // Tokenize document
    val tokenized_df = tokenizer.transform(corpus_df)
//    tokenized_df show 10

    val remover = new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("filtered")
    val filtered_df =remover.transform(tokenized_df)
//    filtered_df show 10

    // Set params for CountVectorizer
    val vectorizer = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setVocabSize(10000)
      .setMinDF(5)
      .fit(filtered_df)

    val countVectors = vectorizer.transform(filtered_df).select("id", "features")
    countVectors.show(10)

    countVectors.take(1).map(println)

//    val lda_countVector = countVectors.map {
//      case Row(id: Long, countVector: SparseVector) => (id, countVector) }
//    lda_countVector.take(1).foreach(println)

    // -------
    val numTopics = 20

    val lda = new LDA()
//      .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
      .setK(numTopics)
//      .setMaxIterations(3)
//      .setDocConcentration(-1) // use default values
//      .setTopicConcentration(-1) // use default values

//    val ldaModel = lda.fit(lda_countVector)
//    ldaModel.describeTopics(5).show()
    val ldaModel = lda.fit(countVectors)
    println("--- Result --->")
    // Review Results of LDA model with Online Variational Bayes
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 5)
    topicIndices.show()
    println("--- 1 --->")
    topicIndices.take(1).map(println)

    val vocabList = vectorizer.vocabulary
    println("--- Final Result --->")
    //scala.collection.mutable.WrappedArray
    topicIndices.foreach { row =>
      row.getList[Int](1).forEach(termIndx => {
        println(vocabList(termIndx))
      })
    }

    println("completed ..")
    spark.close()
  }
}
