package com.yg.horus.dt.jobflow

import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.SparkJobInit
import com.yg.horus.dt.tdm.{TdmMaker, TopicTermManager, Word2vecModeler}
import com.yg.horus.dt.topic.LdaTopicProcessing
import com.yg.horus.dt.total.DtSeriesLoopJobMain
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties

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

class LdaJoblet(spark: SparkSession, seedNo: Long, minAgo: Int, period: Long) extends SerialJoblet(period) {

  override def run(): Unit = {
    val lda = new LdaTopicProcessing(spark)
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(minAgo))
    println(s"Target data to be processed from ${fromTime}")
    val source = lda.loadSource(seedNo, fromTime)

    println("[Source Data for LDA] ----------------- ")
    source.show(3000)

    //    println("----- Topic terms -----")
    val topics = lda.topics(source, 30, 5)

    println("[LDA(30:5)] ----------------- ")
    topics.show(600)

    val fRes = lda.convertObject(topics)

    for (i <- 0 until fRes.length) {
      println(s"Topic #${i}")
      fRes(i).foreach(a => println(a))
      println("--------------")
    }

    lda.saveToDB(topics, seedNo, minAgo)
  }

}

class LdaTdmJoblet(spark: SparkSession, seedNo: Long, minAgo: Int, period: Long)
  extends SerialJoblet(period) {

  override def run(): Unit = {
    val logger = new DtLogger(spark)

    val count = DtLogger.countValue()
    var ts = System.currentTimeMillis()
    runHotLda(seedNo, minAgo)
    logger.logJob("JOBLET_LDA_" + seedNo + "_" + count + "_" + (System.currentTimeMillis() - ts) / 1000, "FIN")

    ts = System.currentTimeMillis()
    runHotTdm(seedNo, minAgo, new TopicTermManager(spark).getTopicsSeq(2), 2)
    logger.logJob("JOBLET_TDM_" + seedNo + "_" + count + "_" + (System.currentTimeMillis() - ts) / 1000, "FIN")

  }

  def runHotLda(seedNo: Long, minAgo: Int) = {
    val lda = new LdaTopicProcessing(spark)
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(minAgo))
    println(s"Target data to be processed from ${fromTime}")
    val source = lda.loadSource(seedNo, fromTime)

    println("[Source Data for LDA] ----------------- ")
    source.show(3000)

    //    println("----- Topic terms -----")
    val topics = lda.topics(source, 30, 5)

    println("[LDA(30:5)] ----------------- ")
    topics.show(600)

    val fRes = lda.convertObject(topics)

    for (i <- 0 until fRes.length) {
      println(s"Topic #${i}")
      fRes(i).foreach(a => println(a))
      println("--------------")
    }

    lda.saveToDB(topics, seedNo, minAgo)
  }


  def runHotTdm(seedNo: Long, minAgo: Int, topics: Seq[String], eachLimit: Int) = {
    val test = new Word2vecModeler(spark)

    val data = test.loadSourceFromMinsAgo(seedNo, minAgo)
    val model = test.createModel(data)

    val tdm = new TdmMaker(spark, model)
    val ts = System.currentTimeMillis()

    topics.foreach(term => {
      try {
        // need to change logic
        tdm.saveToDB(tdm.highTermDistances(term, eachLimit), seedNo, minAgo, ts)
      } catch {
        case _ => println(s"No Terms in Model : ${term}")
      }
    })

    println("Job Finished ..")
  }
}

object SerialJobMain extends SparkJobInit("SERIAL_JOBS") {

  def main(args: Array[String]): Unit = {
    println("Active Serial Job ..")
    implicit def now : Long = System.currentTimeMillis()

    val jobManager = new SeiralJobManager(cntTurns = 300, checkPeriod = 5000L)
    jobManager.addJob(new LdaTdmJoblet(spark, 21, 60, 120000L))
    jobManager.addJob(new LdaTdmJoblet(spark, 25, 120, 300000L))
    jobManager.start2()

    spark.close()
  }
}
