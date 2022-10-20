package com.yg.horus.dt.textrank

import com.yg.horus.dt.SparkJobInit
import com.yg.horus.dt.termcount.DbProcessing
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object TextrankJobMain extends SparkJobInit("DT_TEXT_RANK_JOB") with DbProcessing {
  import spark.implicits._

  val getHighTerms: UserDefinedFunction = udf[Seq[String], String] { doc =>
    try {
      val tr = new TagExtractor(doc)
      println("doc ->" + doc)
      println(tr.extract)
      tr.extract.sortBy(_._1)(Ordering[Double].reverse).take(10).map(_._2).toSeq
    } catch {
      case e: Exception =>
        Seq("NA")
    }
  }

  val getHighTermsString: UserDefinedFunction = udf[String, String] { doc =>
    try {
      val tr = new TagExtractor(doc)
//      println("doc ->" + doc)
//      println(tr.extract)
      tr.extract.sortBy(_._1)(Ordering[Double].reverse).take(5).map(_._2).mkString(", ")
    } catch {
      case e: Exception => "NA"
    }
  }

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    val res = getCrawledData(999, 100)
      .withColumn("TOPIC", getHighTermsString($"PAGE_TEXT"))
//    res.write.csv("./storyTextRankResult.csv")

    res.show(100)

    println("Completed Successfully ..")

  }
}
