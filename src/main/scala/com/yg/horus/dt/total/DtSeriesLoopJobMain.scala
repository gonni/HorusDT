package com.yg.horus.dt.total
import com.yg.horus.RuntimeConfig
import com.yg.horus.dt.SparkJobInit
import com.yg.horus.dt.tdm.{TdmMaker, TopicTermManager, Word2vecModeler}
import com.yg.horus.dt.topic.LdaTopicProcessing
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

class DtSeriesLoopJobMain {

}

object DtSeriesLoopJobMain extends SparkJobInit("DT_INTEGRATED_SERIES_LOOP") {

  case class RunParams(appName: String, master: String, seedNo: Long, minAgo: Int,
                       cntTopic: Int = 10, cntTopicTerms: Int = 10)

  def main(v: Array[String]): Unit = {
    println("Active SparkJob ..")
    displayInitConf()

    val rtParam = v.length match {
      case 6 => RunParams(v(0), v(1), v(2).toLong, v(3).toInt, v(4).toInt, v(5).toInt)
      case _ => RunParams("LDA_TOPIC", RuntimeConfig("spark.master"), 1L, 600)
    }
    println("Run Params => " + rtParam)

    for(i <- 0 to 3) { //TODO need to set by external args
      println(s"Processing UnitJob turn ## ${i}")
      runHotLda(rtParam.seedNo, rtParam.minAgo)
      runHotTdm(rtParam.seedNo, rtParam.minAgo, new TopicTermManager(spark).getTopicsSeq(2))

      println(s"Sleep 5sec ... ${i}")
      //TODO need to calc sleep time
      Thread.sleep(5000L)
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


  def runHotTdm(seedNo: Long, minAgo: Int, topics: Seq[String]) = {
    val test = new Word2vecModeler(spark)

    val data = test.loadSourceFromMinsAgo(seedNo, minAgo)
    val model = test.createModel(data)

    val tdm = new TdmMaker(spark, model)
    val ts = System.currentTimeMillis()

//    val topics = Seq("경제", "사건", "대통령", "주식", "화폐", "사건",
//      "날씨", "북한", "이재명", "금리", "연봉", "코로나", "러시아", "IT",
//      "중국", "미국", "원유", "휘발유", "디젤", "물가", "부동산",
//      "에너지", "공포", "전쟁", "정치")

    topics.foreach(term => {
      try {
        // need to change logic
        tdm.saveToDB(tdm.highTermDistances(term), seedNo, minAgo, ts)
      }catch {
        case _ => println(s"No Terms in Model : ${term}")
      }
    })

    println("Job Finished ..")
  }


}
