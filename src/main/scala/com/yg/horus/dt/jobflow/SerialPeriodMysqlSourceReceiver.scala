package com.yg.horus.dt.jobflow

import com.yg.horus.RuntimeConfig
import org.apache.spark.internal.Logging
import slick.jdbc.MySQLProfile.api._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, desc, monotonically_increasing_id, row_number, typedLit, udf}
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.jdk.CollectionConverters.asScalaBufferConverter
import java.util.Properties
import org.apache.spark.SparkConf

object SyncDbClient {
  protected implicit def executor = scala.concurrent.ExecutionContext.Implicits.global

  val db: Database = Database.forURL(
    url = RuntimeConfig("mysql.url"),
    user = RuntimeConfig("mysql.user"),
    password = RuntimeConfig("mysql.password"),
    driver = "com.mysql.jdbc.Driver"
  )



  def getLatestDstreamUnit(fromTime: java.sql.Timestamp) = {
    ???
  }
}

class SerialPeriodMysqlSourceReceiver(minAgo: Int)
  extends Receiver[DataFrame](StorageLevel.MEMORY_AND_DISK_2) with Logging {



  val prop = new Properties()
  prop.put("user", RuntimeConfig("mysql.user"))
  prop.put("password", RuntimeConfig("mysql.password"))

//  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
//  komoran.setUserDic(RuntimeConfig("komoran.dic"))

  override def onStart() = {
    new Thread("MysqlDStream") {


      override def run() = {
        generateAction()
      }
    }.start()
  }

  private def generateAction() = {
    val conf = new SparkConf().setMaster(RuntimeConfig("spark.master")).setAppName("SUB_MYSQL_STREAM_RCV")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
//    val getTokenListUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
//      try {
//        komoran.analyze(sentence).getNouns.asScala
//      } catch {
//        case e: Exception => {
//          println("Detected Null Pointer .. " + e.getMessage)
//          Seq()
//        }
//      }
//    }

    println("--------------------------------> SparkContext :" + spark.sparkContext)

    while(!isStopped) {
      try {
        if(spark != null) {
          val fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(minAgo))
          println("Create/Get DStream FromTime :" + fromTime)

          println("---------------spark.read :" + spark.read )
          val tableDf = spark.read.jdbc(RuntimeConfig("mysql.url"), "crawl_unit1", prop)
//          val data = tableDf.filter($"REG_DATE" > fromTime
//            && $"STATUS" === "SUCC" && $"PAGE_TEXT".notEqual("null"))
//            .orderBy(desc("CRAWL_NO"))
//            .select($"ANCHOR_TEXT", $"PAGE_TEXT", $"CRAWL_NO")
//            .withColumnRenamed("CRAWL_NO", "id")
          //          .withColumn("tokenized", getTokenListUdf2($"PAGE_TEXT"))
          //        data.show()

          store(tableDf)
        } else {
          println("spark session is null ..")
        }
        Thread.sleep(5000L)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          Thread.sleep(5000L)
        }
      }
    }
  }

  override def onStop(): Unit = ???

}
