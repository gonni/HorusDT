//package com.yg.horus.dt.vec
//
//import com.yg.horus.RuntimeConfig
//import com.yg.horus.dt.db.CrawlRepo
//import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor
//import org.deeplearning4j.text.sentenceiterator.labelaware.LabelAwareSentenceIterator
//
//import java.util
//import slick.jdbc.MySQLProfile.api._
//
//import scala.concurrent.Await
//import scala.concurrent.duration.DurationInt
//import scala.collection.JavaConverters._
//
//trait DbCrawledData {
//  protected implicit def executor = scala.concurrent.ExecutionContext.Implicits.global
//
//  val db : Database = Database.forURL(
//    url = RuntimeConfig("mysql.url"),
//    user = RuntimeConfig("mysql.user"),
//    password = RuntimeConfig("mysql.password"),
//    driver = "com.mysql.jdbc.Driver")
//
//  def getData(limit: Int) = {
//    Await.result(db.run(CrawlRepo.findCrawled(1, limit).result), 10.seconds)
//  }
//
//  def allData(minSeedNo: Int) = {
//    Await.result(db.run(CrawlRepo.findCrawledData(minSeedNo).result), 100.seconds)
//  }
//
//  def countAllData(minSeedNo: Int) = {
//    Await.result(db.run(CrawlRepo.countCrawledData(minSeedNo).result), 10.seconds)
//  }
//}
//
//class DbLabelAwareIterator(val seedNo: Int, val limit: Int) extends LabelAwareSentenceIterator {
//  protected implicit def executor = scala.concurrent.ExecutionContext.Implicits.global
//
//  val db : Database = Database.forURL(
//    url = RuntimeConfig("mysql.url"),
//    user = RuntimeConfig("mysql.user"),
//    password = RuntimeConfig("mysql.password"),
//    driver = "com.mysql.jdbc.Driver")
//
//  private def getData() = {
//    Await.result(db.run(CrawlRepo.findCrawled(1, limit).result), 10.seconds)
//  }
//
//  private val _loadedData = getData()
//  println("loac data : " + _loadedData.length)
//  private var _pointer = -1
//  private var _sentencePreProcessor : SentencePreProcessor = _
//
//  override def currentLabel(): String = {
//    "DOC_" + this._loadedData(_pointer).seedNo.toString
//  }
//
//  override def currentLabels(): util.List[String] = {
//    List(currentLabel()).asJava
//  }
//
//  override def nextSentence(): String = {
//    hasNext match {
//      case true => {
//        _pointer += 1
//        val a = _loadedData(_pointer).pageText.getOrElse(null)
//        a
//      }
//      case _ =>
//        null
//    }
//  }
//
//  override def hasNext: Boolean = _pointer + 1 < _loadedData.length
//
//  override def reset(): Unit = _pointer = 0
//
//  override def finish(): Unit = println("Just finished")
//
////  override def getPreProcessor: SentencePreProcessor = (sentence: String) => sentence.trim
//  override def getPreProcessor: SentencePreProcessor = _sentencePreProcessor
//  override def setPreProcessor(preProcessor: SentencePreProcessor
//                               = (sentence: String) => sentence.trim): Unit =
//    _sentencePreProcessor = preProcessor
//}
//
//object DbLabelAwareIterator {
//  def main(args: Array[String]): Unit = {
//    println("Active System ..")
//
//    val test = new DbLabelAwareIterator(1, 30)
//    test.getData().foreach(println)
//
//    println("---------------------------------------------")
//
//    var i = 0
//    while(test.hasNext) {
//      println(i + "=>" + test.nextSentence())
//      i += 1
//    }
//
//  }
//}