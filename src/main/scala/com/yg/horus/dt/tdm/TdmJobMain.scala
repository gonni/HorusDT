package com.yg.horus.dt.tdm

import com.yg.horus.RuntimeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Word2vec:Job -- (model created) --> TDM:Job
 * Arguments : appName, sparkMasterUrl, seedNo, minAgo
 */
object TdmJobMain {
  case class RunParams(appName: String, master: String, seedNo: Long, minAgo: Int)

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    println("--------------------------------------")
    println("Active Profile :" + RuntimeConfig("profile.name"))
    println("--------------------------------------")
    println("ConfigDetails : " + RuntimeConfig())

    val rparam = args.length match {
      case 4 => RunParams(args(0), args(1), args(2).toLong, args(3).toInt)
      case _ => RunParams("MODEL_TDM", RuntimeConfig("spark.master"), 21L, 600)
    }

    println("Applied Params :" + rparam)

    val conf = new SparkConf().setMaster(rparam.master).setAppName(rparam.appName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new Word2vecModeler(spark)

    val data = test.loadSourceFromMinsAgo(rparam.seedNo, rparam.minAgo)
    val model = test.createModel(data)

//    model.findSynonyms("김", 30).show(100)
//    println("Create TDM ..")

    val tdm = new TdmMaker(spark, model)
//    val rs = tdm.highTermDistances("김")
//    rs.show()

    val ts = System.currentTimeMillis()

    val topics = Seq("경제", "사건", "대통령", "주식", "화폐", "사건",
      "날씨", "북한", "이재명", "금리", "연봉", "코로나", "러시아", "IT",
      "중국", "미국", "원유", "휘발유", "디젤", "물가", "부동산",
      "에너지", "공포", "전쟁", "정치")

    topics.foreach(term => {
      try {
        tdm.saveToDB(tdm.highTermDistances(term), rparam.seedNo, rparam.minAgo, ts)
      }catch {
        case _ => println(s"No Terms in Model : ${term}")
      }
    })

    println("Job Finished ..")

    spark.close()
  }
}
