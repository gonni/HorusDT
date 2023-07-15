package com.yg.horus.dt.termcount

import com.yg.horus.RuntimeConfig
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object DbUtil {
  protected implicit def executor = scala.concurrent.ExecutionContext.Implicits.global

  val conf = RuntimeConfig.getRuntimeConfig()
//  val db : Database = Database.forURL(url ="jdbc:mysql://192.168.35.123:3306/horus?useSSL=false",
//  val db : Database = Database.forURL(url ="jdbc:mysql://localhost:3306/horus?useSSL=false",
//    user="root", password="18651865", driver = "com.mysql.jdbc.Driver")

  val db : Database = Database.forURL(
    url = conf.getString("mysql.url"),
    user = conf.getString("mysql.user"),
    password = conf.getString("mysql.password"),
    driver = "com.mysql.cj.jdbc.Driver")

  val getLatestAnchorWithLimit = (seedNo: Long, startCrawlNo: Long, limit : Int) =>
    Await.result(db.run(CrawledRepo.findLatestAnchor(seedNo, startCrawlNo, limit).result), 10.seconds)

//  val getLatestAnchor = Await.result(db.run(CrawledRepo.findLatestAnchor(21L).result), 10.seconds)
  val getMaxCrawlNo = (seedNo: Long) => Await.result(db.run(CrawledRepo.findLatestCrawlNo(seedNo).result), 10.seconds).getOrElse(0L)

  def latestCrawlNo(seedNo: Long) : Long = {
    Await.result(db.run(CrawledRepo.findLatestCrawlNo(seedNo).result), 10.seconds).getOrElse(0L)
  }

  def getLatestAnchorFrom(seedNo: Long, startCrawlNo: Long) = {
    Await.result(db.run(CrawledRepo.findLatestAnchor(seedNo, startCrawlNo, 100).result), 10.seconds)
  }

  def getLatestContextFrom(seedNo: Long, startCrawlNo: Long) = {
    Await.result(db.run(CrawledRepo.findLatestContent(seedNo, startCrawlNo, 100).result), 10.seconds)
  }

  def getAllLatestContext(startCrawlNo: Long) = {
    Await.result(db.run(CrawledRepo.findAllLatestContent(startCrawlNo, 1000).result), 10.seconds)
  }

//  val getLatestAnchorFrom = (startCrawlNo: Long) => Await.result(db.run(CrawledRepo.findLatestAnchor(21L).result), 10.seconds)

  def main(args: Array[String]): Unit = {
//    getLatestAnchorWithLimit(21L,1L,10).foreach(anchor => {
//      println(anchor)
//    })
//    println("LatestCrawlNo ->" + getMaxCrawlNo(9L))
//
//    println("Cont =>" + getLatestContextFrom(9L, 361830L).map(_.getOrElse("NULL")).mkString("\n\n"))

    println("latest crawlNo 1-> " + latestCrawlNo(1L))
    println("latest crawlNo 2-> " + latestCrawlNo(2L))

//    getAllLatestContext(1556564).foreach(println)



//      .foreach(a => {
//      println(a)
//    })

//    var target = Seq[Int](1,2,3,5,6)
//    target = target :+ 7
//    println(target)
  }
}

class MySqlSourceReceiver(val seedNo : Long) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)
  with Logging {

  var latestCrawlNo = 0L

  override def onStart(): Unit = {
    new Thread("MysqlSt") {
      override def run(): Unit = {
        createGetData
      }
    }.start()
  }

  override def onStop(): Unit = synchronized {
//    this.db.close()
  }

  private def createGetData(): Unit = {
    while(!isStopped) {
      try {
        val res = DbUtil.getLatestContextFrom(seedNo, latestCrawlNo)
        println(s"Count of crawled data : ${res.size} for seed#${seedNo}")

        val mergedRes = DbUtil.getLatestContextFrom(seedNo, latestCrawlNo).mkString("\n\n")
        println(mergedRes)
        store(mergedRes)

        latestCrawlNo = DbUtil.latestCrawlNo(seedNo)
        println(s"Update Point ${latestCrawlNo} for seed#${seedNo}")

        Thread.sleep(5000)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}

class TimePeriodMysqlSourceReceiver(seedNos: Seq[Long]) extends Receiver[(Long, String)](StorageLevel.MEMORY_AND_DISK_2)
  with Logging {

  var latestCrawlNo = 0L

  override def onStart(): Unit = {
    latestCrawlNo = DbUtil.latestCrawlNo(2L)
    println("--------------> Set Latest CrawlNo :" + latestCrawlNo)

    new Thread("MysqlSt") {
      override def run(): Unit = {
        createGetData
      }
    }.start()
  }

  override def onStop(): Unit = synchronized {
    //    this.db.close()
  }

  private def createGetData(): Unit = {
    while (!isStopped) {
      try {
        val res = DbUtil.getAllLatestContext(latestCrawlNo)
        println(s"Count of crawled data : ${res.size} for seed#All")

        for(seedNo <- seedNos) {
          val strAllData = res.filter(_.seedNo == seedNo).map(row =>
            row.anchorText.getOrElse(" ")  + "\n" + row.pageText.getOrElse(" "))
            .mkString("\n\n")

          if(strAllData != null && strAllData.trim.length > 0)
            store((seedNo, strAllData))
        }

        latestCrawlNo = res.map(_.crawlNo).headOption.getOrElse(latestCrawlNo)
        println(s"Update Start Crawl Point : ${latestCrawlNo} for seed#All")

        Thread.sleep(5000)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

}
