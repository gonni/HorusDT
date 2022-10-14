package com.yg.horus.dt.total

import com.yg.horus.dt.SparkJobInit
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.yg.horus.dt.tdm.{TdmMaker, TopicTermManager, Word2vecModeler}
import com.yg.horus.dt.topic.LdaTopicProcessing

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties
import scala.collection.mutable

class SeedJob(val seedId: Long, val period: Long, spark: SparkSession, var remain: Long = 0) extends DtSeriesLoopJobMain(spark) {
  def isRunning(offsetTime: Long) = {
    println("remain :" + remain)
    if(remain - offsetTime < 0) {
      remain = period
      true
    }
    else {
      remain -= period
      false
    }
  }

  def runHotLda(seedNo: Long, minAgo: Int) = {
    val lda = new LdaTopicProcessing(spark)
    val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(minAgo))
    println(s"Target data to be processed from ${fromTime}")
    val source = lda.loadSource(seedNo, fromTime)

    source.show()

    //    println("----- Topic terms -----")
    val topics = lda.topics(source, 10, 15)
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


object LoopJobManager extends SparkJobInit("DT_LOOP_JOB_MANAGER") {

//  var seedJobs = mutable.MutableList[(Long, Long)]()

  def main(args: Array[String]): Unit = {
    println("Active System ..")
    val jobProcessor = new SeedJob(1, 1000, spark)

    println(jobProcessor.isRunning(999))
    println(jobProcessor.isRunning(999))
    println(jobProcessor.isRunning(999))
    println(jobProcessor.isRunning(9))
    println(jobProcessor.isRunning(9))
    println(jobProcessor.isRunning(999))

//    seedJobs ++= List((1, 1000), (2,2000), (3,9000), (5,3000), (7,2000))
//    for(turn <- 0 to 10) {
//      println(turn + "-------------------")
//      seedJobs = seedJobs.map(a => (a._1, a._2 - 100))
//      seedJobs.foreach(println)
    }
}
