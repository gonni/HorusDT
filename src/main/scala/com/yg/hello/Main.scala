package com.yg.hello

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.collection.JavaConverters._

object Main {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

  val getPlainTextUdf: UserDefinedFunction = udf[String, String] { sentence =>
    komoran.analyze(sentence).getPlainText
  }

  val getPlainTextUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    //    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    komoran.analyze(sentence).getPlainText.split("\\s")
  }

  val getNounsUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getNouns.asScala
  }

  val getTokenListUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getTokenList.asScala.map(x => x.toString)
  }

  val getTokenListUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getTokenList.asScala.map(token => token.getMorph)
  }

  val func = udf((s:String) => if(s.length > 30) "length:" + s.length else s)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SuperHell5")
      .setMaster("spark://spark:7077")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    import spark.implicits._


    print("\n\n>>>>> START OF PROGRAM <<<<<\n\n")

    println("Hello World.")

    print("\n\n>>>>> END OF PROGRAM <<<<<\n\n")

    val testDataset = spark.createDataFrame(Seq(
      "밀리언 달러 베이비랑 바람과 함께 사라지다랑 뭐가 더 재밌었어?",
      "아버지가방에들어가신다",
      "나는 밥을 먹는다",
      "하늘을 나는 자동차",
      "아이폰 기다리다 지쳐 애플공홈에서 언락폰질러버렸다 6+ 128기가실버ㅋ"
    ).map(Tuple1.apply)).toDF("sentence")

    // 1. print test data
    testDataset.show(truncate = false)

    val analyzedDataset =
      testDataset.withColumn("plain_text", getPlainTextUdf($"sentence"))
        .withColumn("nouns", getNounsUdf($"sentence"))
        .withColumn("token_list", getTokenListUdf2($"sentence"))
        .withColumn("plain_text2", getPlainTextUdf2($"sentence"))

    // 2. print test data and analyzed result as list
    analyzedDataset.select("sentence", "token_list").show()
    analyzedDataset.select("sentence", "plain_text").write.csv("/usr/local/spark/resources/datacsv_" + System.currentTimeMillis())

    spark.close()
  }
}
