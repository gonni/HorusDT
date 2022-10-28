package com.yg.horus.dt.topic

import com.yg.horus.RuntimeConfig
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, desc, monotonically_increasing_id, row_number, typedLit, udf}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaBufferConverter
import Array._
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable

case class TopicTermScore(term: String, score: Double)
class LdaTopicProcessing(val spark: SparkSession) {
  import spark.implicits._

  def saveTopic2Data() = {

  }

  def convertObject(topics : Dataset[mutable.WrappedArray[(String, Double)]]) = {
    val res = topics.map(row => {
      row.map(ts => TopicTermScore(ts._1, ts._2)).toSeq
    }).collect()
    res
  }

  def saveToDB(topics : Dataset[mutable.WrappedArray[(String, Double)]], seedNo: Long, minAgo: Int) = {
    val res = convertObject(topics)

    val lstRes = res.flatMap(topicRow => {
      topicRow.map { ts => {
        (res.indexOf(topicRow), ts.term, ts.score)
        }
      }
    }).toSeq

//    println("Topic Size => " + lstRes.size)

    val ts = System.currentTimeMillis()
    val resDf = lstRes.toDF("TOPIC_NO", "TERM", "SCORE")
      .withColumn("START_MIN_AGO", typedLit(minAgo))
      .withColumn("SEED_NO", typedLit(seedNo))
      .withColumn("GRP_TS", typedLit(ts))

    resDf.show()

    val prop = new Properties()
    prop.put("user", RuntimeConfig("mysql.user"))
    prop.put("password", RuntimeConfig("mysql.password"))

    resDf.write.mode(SaveMode.Append).jdbc(RuntimeConfig("spark.jobs.lda.writeDB"),
      "DT_LDA_TOPICS", prop)
  }

  def topics(dfTokenized: DataFrame, countOfTopics: Int, cntTermsForTopic: Int,
             write: Dataset[mutable.WrappedArray[(String, Double)]] => Unit = (_) => println("No Write Chain")) = {
    val vectorizer = new CountVectorizer()
      .setInputCol("tokenized")
      .setOutputCol("features")
      .setVocabSize(10000)
      .setMinDF(5)
      .fit(dfTokenized)

    val countVectors = vectorizer.transform(dfTokenized).select("id", "features")
    val lda = new LDA().setK(countOfTopics)
    val ldaModel = lda.fit(countVectors)
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = cntTermsForTopic)
    val vocabList = vectorizer.vocabulary
    val topics = topicIndices.map{ row =>
      row.getAs[mutable.WrappedArray[Int]](1).map(vocabList(_))
        .zip(row.getAs[mutable.WrappedArray[Double]](2))
    }

    write(topics)
    topics
  }

  def loadSource(seedNo: Long, fromTime: java.sql.Timestamp) = {
    val getTokenListUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
      try {
//        LdaTopicProcessing.komoran.analyze(sentence).getTokenList.asScala.map(_.getMorph)
        LdaTopicProcessing.komoran.analyze(sentence).getNouns.asScala
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

    val tableDf = spark.read.jdbc(RuntimeConfig("mysql.url"), "crawl_unit1", prop)
    tableDf.filter($"SEED_NO" === seedNo && $"REG_DATE" > fromTime
      && $"STATUS" === "SUCC" && $"PAGE_TEXT".notEqual("null"))
      .orderBy(desc("CRAWL_NO"))
      .select($"ANCHOR_TEXT", $"PAGE_TEXT", $"CRAWL_NO")
      .withColumnRenamed("CRAWL_NO", "id")
      .withColumn("tokenized", getTokenListUdf2($"PAGE_TEXT"))
  }
}


object LdaTopicProcessing {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
  komoran.setUserDic(RuntimeConfig("komoran.dic"))
//  komoran.setUserDic("/Users/ygkim/IdeaProjects/HorusDT/myDic.txt")

  def main(args: Array[String]): Unit = {
    println("Active System ..")
    komoran.analyze("윤석렬 대통령은 김건희 여사에게 한방을 말했다.").getNouns.asScala.foreach(println)
  }

  def sample() = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("text lda")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    println("Start ..")
    val dataset = spark.read.format("libsvm")
      .load("data/sample_lda_libsvm_data.txt")
    dataset.take(1).map(println)

    println("Finished ..")
    spark.close()
  }
}
