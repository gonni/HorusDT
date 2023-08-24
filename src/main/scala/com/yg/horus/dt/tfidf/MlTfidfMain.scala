package com.yg.horus.dt.tfidf
import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.tfidf.DocTfIDFJobMain.TfidfParam
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MlTfidfMain {

  def main(v: Array[String]): Unit = {
    println("--------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("--------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())

    val runParams = v.length match {
      case 4 => TfidfParam(v(0), v(1), v(2).toLong, v(3).toInt)
      case _ =>
        if (RuntimeConfig.getActiveProfile().contains("dev"))
          TfidfParam(seedId = 21L, limit = 20000)
        else
          TfidfParam(seedId = 21L, limit = 20000) //TODO
    }
    println("--------------------------------------")
    println(s"TF-IDF Job Args : ${runParams}")
    println("--------------------------------------")

    val conf = new SparkConf().setMaster(runParams.master).setAppName(runParams.appName)
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    // Load documents (one per line).
    val documents: RDD[Seq[String]] = spark.sparkContext.textFile("data/mllib/kmeans_data.txt")
      .map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)
  }
}
