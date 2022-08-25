package com.yg.horus.dt.topic

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.LDA

object Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("text lda")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

//    val sc = spark.sparkContext

    println("Start ..")
    val dataset = spark.read.format("libsvm")
      .load("data/sample_lda_libsvm_data.txt")
    dataset.take(1).map(println)

    println("Finished ..")
    spark.close()
  }
}
