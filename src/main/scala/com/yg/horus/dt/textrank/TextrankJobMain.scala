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
    val tr = new TagExtractor(doc)
    tr.extract.sortBy(_._1)(Ordering[Double].reverse).take(5).map(_._2).toSeq
//    Seq("A", "B", "C")
  }

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    getCrawledData(999, 100)
      .withColumn("HIGH_RANK", getHighTerms($"PAGE_TEXT"))
      .show()

  }
}
