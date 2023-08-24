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
      case _ => RunParams("MODEL_TDM", RuntimeConfig("spark.master"), 21L, 3600 * 24 * 7)
    }

    println("Applied Params :" + rparam)

    val conf = new SparkConf().setMaster(rparam.master).setAppName(rparam.appName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val test = new Word2vecModeler(spark)

    val data = test.loadSourceFromMinsAgo(rparam.seedNo, rparam.minAgo)
    val model = test.createModel(data)

    model.findSynonyms("날씨", 30).show(100)
    println("Create TDM ..")

    val tdm = new TdmMaker(spark)
//    val rs = tdm.highTermDistances("김")
//    rs.show()

    val ts = System.currentTimeMillis()

    val topics = Seq("경제", "사건", "대통령", "주식", "화폐", "사건",
      "날씨", "북한", "이재명", "윤석열", "금리", "연봉", "코로나", "러시아", "IT",
      "중국", "미국", "원유", "휘발유", "디젤", "물가", "부동산",
      "에너지", "공포", "전쟁", "정치", "피해", "전쟁", "경제", "일본", "민주당", "경찰", "우크라이나", "러시아", "사고", "지역", "부산", "서울",
      "여성", "세계", "가격", "산업", "상승", "아파트", "반도체", "국민의힘", "광주", "축제", "대회", "은행", "인구", "연구",
      "남성", "의료", "마약", "교사", "학교", "영업", "군", "경기", "강원", "주택", "19", "정치", "수출", "후쿠시마", "국제", "브랜드", "주가",
      "에너지", "범행", "잼버리", "미사일", "를로벌", "삼성전자", "범죄", "공항", "해양"
    )

    topics.foreach(term => {
      try {
        // need to change logic
        tdm.saveToTable(tdm.highTermDistances(model, term, 30), rparam.seedNo, rparam.minAgo, ts)
      }catch {
        case _ => println(s"No Terms in Model : ${term}")
      }
    })

    println("Job Finished ..")

    spark.close()
  }
}
