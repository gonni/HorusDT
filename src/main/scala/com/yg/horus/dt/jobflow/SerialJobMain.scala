package com.yg.horus.dt.jobflow

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.SparkJobInit
import com.yg.horus.dt.jobflow.SerialJobMain.spark
import com.yg.horus.dt.tdm.TopicTermManager.spark
import com.yg.horus.dt.tdm.{TdmMaker, TopicTermManager, Word2vecModeler}
import com.yg.horus.dt.topic.LdaTopicProcessing
import com.yg.horus.dt.total.DtSeriesLoopJobMain
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties
import scala.collection.mutable.ListBuffer

object DtLogger {
  var count = 0
  def countValue() = {
    count += 1
    count
  }
}

class DtLogger(spark: SparkSession) {
  import spark.implicits._

  val prop = new Properties()
  prop.put("user", RuntimeConfig().getString("mysql.user"))
  prop.put("password", RuntimeConfig().getString("mysql.password"))

  def logJob(jobName: String, status: String) = {
    val dataDf = Seq((jobName, status)).toDF("JOB_NAME", "STATUS")
    dataDf show

    dataDf.write.mode(SaveMode.Append).jdbc(RuntimeConfig("spark.jobs.lda.writeDB"),
      "DT_JOB_LOG", prop)
  }
}
case class TopicTdm(baseTerm: String, nearTerm: String, topicScore: Double, seedNo: Int, gprTs: Long)

class LdaTdmJoblet(spark: SparkSession, seedNo: Long, minAgo: Int, period: Long)
  extends SerialJoblet(period) {
  val logger = new DtLogger(spark)

  val lda = new LdaTopicProcessing(spark)
  val w2vModeler = new Word2vecModeler(spark)
  val topicTermManager = new TopicTermManager(spark)
  val tdm = new TdmMaker(spark)

  override def run(): Unit = {
    val count = DtLogger.countValue()
    var ts = System.currentTimeMillis()
    try {
      runHotLda(seedNo, minAgo, logger)
      logger.logJob("JOBLET_LDA_" + seedNo + "_" + count + "_" + (System.currentTimeMillis() - ts) / 1000, "FIN")

      ts = System.currentTimeMillis()
      runHotTdm(seedNo, minAgo, new TopicTermManager(spark).getLatestTopicsSeq(seedNo, 2), 10, logger)
      logger.logJob("JOBLET_TDM_" + seedNo + "_" + count + "_" + (System.currentTimeMillis() - ts) / 1000, "FIN")

      spark.catalog.clearCache()
      spark.sqlContext.clearCache()
//      spark.sparkContext.clearJobGroup()

      println("cache cleaned ----------------------------------------------")
    } catch {
      case e: Exception => {
        logger.logJob("JOBLET_LDATDM_ERROR_"  + seedNo + "_" + count + "_" + (System.currentTimeMillis() - ts) / 1000, "FIN")
        e.printStackTrace()
      }
    }

  }

  def runHotMergedTdm(seedNo: Int, model: Word2VecModel, grpTs: Long) = {
    import spark.implicits._
    val mergedTopics = topicTermManager.getMergedTopicSeq(seedNo, 10, 10)

    val res = mergedTopics.map(topic => {
      val strNearTerms = tdm.strNearTermsOnVectorIn(model, topic.topicName, 7)
      TopicTdm(topic.topicName, strNearTerms, topic.score, seedNo, grpTs)

    })

    val resDf = res.toDF("BASE_TERM", "NEAR_TERM", "TOPIC_SCORE", "SEED_NO", "GRP_TS")
    resDf.show

    tdm.saveMergedTopicTdm(resDf)

    resDf.unpersist(blocking = true)
  }

  def runHotLda(seedNo: Long, minAgo: Int, logger: DtLogger) = {
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(minAgo))
    println(s"Target data to be processed from ${fromTime}")
    val source = lda.loadSource(seedNo, fromTime)

    println("[Source Data for LDA] ----------------- ")
    source.show(300)

    if(source.count() < 10) {
      println("==> Crawled data for LDA is not enough .." + source.count())
      logger.logJob("JOBLET_LDA_" + seedNo + "_NOT_ENOUGH_DATA" , "NA")
    } else {
      val topics = lda.topics(source, 10, 10)
      topics.show(100)

      lda.saveToDB(topics, seedNo, minAgo)

      topics.unpersist(blocking = true)
    }
    source.unpersist(blocking = true)
  }

  def runHotTdm(seedNo: Long, minAgo: Int, topics: Seq[String], eachLimit: Int, logger: DtLogger) = {
    val data = w2vModeler.loadSourceFromMinsAgo(seedNo, minAgo)
    if (data.count() < 10) {
      println("==> Crawled data for TDM is not enough .." + data.count())
      logger.logJob("JOBLET_TDM_" + seedNo + "_NOT_ENOUGH_DATA" , "NA")
    } else {
      println("processing TDM .. " + seedNo)
      val model = w2vModeler.createModel(data)

      val ts = System.currentTimeMillis()

      topics.foreach(term => {
        try {
          // need to change logic
          tdm.saveToDB(tdm.highTermDistances(model, term, eachLimit), seedNo, minAgo, ts)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      })

      runHotMergedTdm(seedNo.toInt, model, ts)
      println("TDM Job Finished ..")
    }
    data.unpersist(blocking = true)
  }
}

object SerialJobMain extends SparkJobInit("SERIAL_JOBS") {
  implicit def k(v: Int) : Kilo = new Kilo(v)


  class Kilo(val x: Int) {
    def k(): Long = x * 1000L
  }


  def main(args: Array[String]): Unit = {
    println("Active Serial Job ..")

    implicit def now : Long = System.currentTimeMillis()

    println("------------------------------------------------")
    println("Active Profile : " + RuntimeConfig("profile.name"))
    println("------------------------------------------------")
    println("RuntimeConfig Details : " + RuntimeConfig())

    val jobManager = new SeiralJobManager(cntTurns = 300000, checkPeriod = 5000L)
    jobManager.addJob(new LdaTdmJoblet(spark, 21, 60, 120 k))
    jobManager.addJob(new LdaTdmJoblet(spark, 25, 60, 240 k))
    jobManager.addJob(new LdaTdmJoblet(spark, 23, 60, 300 k))

//      jobManager.addJob(new LdaTdmJoblet(spark, 1, 600, 120 k))
//      jobManager.addJob(new LdaTdmJoblet(spark, 2, 60, 300 k))

    jobManager.start()

//    val w2vModeler = new Word2vecModeler(spark)
//    val data = w2vModeler.loadSourceFromMinsAgo(21L, 60)
//    val model = w2vModeler.createModel(data)
//

//    Seq(TopicTdm("A1", "B1", 12.1, 21, 1L)).toDF("BASE_TERM", "NEAR_TERM", "TOPIC_SCORE", "SEED_NO", "GRP_TS").show
//    val w2vModeler = new Word2vecModeler(spark)
//    val model = Word2VecModel.load("data/w2vNews2Cont_v200_m8_w7_it8")
//    runHotMergedTdm(21, new DtLogger(spark), model, 18L)

    spark.close()
  }
}
