package com.yg.horus.dt.total
import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.SparkJobInit
import com.yg.horus.dt.tdm.{TdmMaker, TopicTermManager, Word2vecModeler}
import com.yg.horus.dt.topic.LdaTopicProcessing
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties

class DtSeriesLoopJobMain(spark: SparkSession) {
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

object DtSeriesLoopJobMain extends SparkJobInit("DT_INTEGRATED_SERIES_LOOP") {

  case class RunParams(master: String = RuntimeConfig("spark.master"),
                       seedNo: Long,
                       minAgo: Int,
                       cntTopic: Int = 10,
                       cntTopicTerms: Int = 10,
                       tdmEachLimit: Int = 20,
                       loopPeriod: Long = 180000L,
                       allTurn: Int = 10000
                      )

  def main(v: Array[String]): Unit = {
    println("Active SparkJob ..")
    displayInitConf()

    val rtParam = v.length match {
      case 6 => RunParams(v(0), v(1).toLong, v(2).toInt, v(3).toInt, v(4).toInt, v(5).toInt, v(6).toLong)
      case 4 => RunParams(seedNo = v(0).toLong, minAgo = v(1).toInt, loopPeriod = v(2).toLong,
        allTurn = v(3).toInt)
      case _ => RunParams(seedNo = 999L, minAgo = 60000, loopPeriod = 0, allTurn = 1)
    }
    println("Run Params => " + rtParam)

    val logger = new DtSeriesLoopJobMain(spark)
    logger.logJob("START_LOOP_JOB", "GOOD")

    var ts = 0L
    var ts1 = 0L
    for(i <- 0 to rtParam.allTurn) { //TODO need to set by external args
      try {
        println(s"Processing UnitJob turn ## ${i}")
        ts = System.currentTimeMillis()

        runHotLda(rtParam.seedNo, rtParam.minAgo)
        logger.logJob("HOT_LDA_" + i + "_" + (System.currentTimeMillis() - ts) / 1000, "FIN")

        ts1 = System.currentTimeMillis()
        runHotTdm(
          rtParam.seedNo,
          rtParam.minAgo,
          new TopicTermManager(spark).getTopicsSeq(2),
          rtParam.tdmEachLimit)
        logger.logJob("HOT_TDM_" + i + "_" + (System.currentTimeMillis() - ts1) / 1000, "FIN")

        var dts = rtParam.loopPeriod - (System.currentTimeMillis() - ts)
        logger.logJob("SLEEP_" + dts, "GOOD")

        if (dts > 0) {
          println(s"Sleep for delta period for ${dts}")
          Thread.sleep(dts)
        }
      } catch {
        case e: Exception => {
          println("Detected Critical Error ..")
          Thread.sleep(10000L)
        }
      }
    }

    logger.logJob("FINISHED_LOOP", "SUCC")
    spark.close()
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

    for(i <- 0 until fRes.length) {
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
      }catch {
        case _ => println(s"No Terms in Model : ${term}")
      }
    })

    println("Job Finished ..")
  }


}
