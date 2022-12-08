package com.yg.horus.dt.vec

import com.yg.horus.RuntimeConfig
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import org.deeplearning4j.models.paragraphvectors._
import org.deeplearning4j.text.documentiterator.{LabelledDocument, SimpleLabelAwareIterator}
import org.deeplearning4j.text.tokenization.tokenizer.{TokenPreProcess, Tokenizer}
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.util
import scala.jdk.CollectionConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import kr.co.shineware.nlp.komoran.core.Komoran
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer

class KomoranTokenFactory extends TokenizerFactory {

  val komoran = new Komoran(DEFAULT_MODEL.FULL)

  override def create(toTokenize: String): Tokenizer = new Tokenizer {

    val tokens = try(
      komoran.analyze(toTokenize).getTokenList.asScala) catch {case _ => List()}
    var pointer = 0

    override def hasMoreTokens: Boolean = pointer < tokens.length

    override def countTokens(): Int = tokens.length

    override def nextToken(): String = {
      val v = tokens(pointer)
      pointer += 1
      v.getMorph
    }

    override def getTokens: util.List[String] = tokens.map(_.getMorph).asJava

    override def setTokenPreProcessor(tokenPreProcessor: TokenPreProcess): Unit = {
      println("Not implemented ..")
    }
  }

  override def create(toTokenize: InputStream): Tokenizer = {
    val br = new BufferedReader(new InputStreamReader(toTokenize))
    val src = br.lines().reduce((a, b) => a + "\n" + b)
    br.close()
    create(src.get)
  }

  override def setTokenPreProcessor(preProcessor: TokenPreProcess): Unit = ???

  override def getTokenPreProcessor: TokenPreProcess = (token: String) => token.trim
}

class Doc2vecSample extends DbCrawledData

object Doc2vecSample extends DbCrawledData {

  def createModel() = {
//    val test = new Doc2vecSample
//    //    test.getData(100).foreach(println)
//
////    val docs = test.getData(500).map(item => {
////      val doc = new LabelledDocument
////      doc.setContent(item.pageText.get)
////      doc.setLabel("DOC_" + item.crawlNo + "_" + item.anchorText)
////      doc
////    }).asJava
//    val docs = test.allData(199).map(item => {
//      val doc = new LabelledDocument
//      doc.setContent(item.pageText.getOrElse(""))
//      doc.setLabel("DOC_" + item.crawlNo + "_" + item.anchorText)
//      doc
//    }).asJava
//
//    println("Size of Doc :"  + docs.size())
//
////    docs.forEach(println)
//
//    val iter = new SimpleLabelAwareIterator(docs)
//

    val korTokenizerFac = new KomoranTokenFactory()
//    //    val iter = new DbLabelAwareIterator(1, 300)
//
//    val vec = new ParagraphVectors.Builder()
//      .minWordFrequency(5)
//      .layerSize(200)
//      .stopWords(new util.ArrayList[String]())
//      .windowSize(7)
//      .iterate(iter)
//      .tokenizerFactory(korTokenizerFac)
//      .build()
//
//    vec.fit()

//    WordVectorSerializer.writeParagraphVectors(vec, "./story_prod_all.mdl")

    // 2) ..
    val vec = WordVectorSerializer.readParagraphVectors("./story_prod_all.mdl")
    vec.setTokenizerFactory(korTokenizerFac)

    println("====================================")
    println("Ext Labels => " + vec.extractLabels())

    println("--------------------<B>-------------------")
    util.Arrays.asList("무협")
    vec.wordsNearest(util.Arrays.asList("무협", "가문", "강호", "검", "도", "독종"),
      util.Arrays.asList("천재", "난세", "영웅"), 1000).forEach(println)

    println("--------------------<A>-------------------")
    vec.nearestLabels("무협 가문 강호 검 도 독종", 10).forEach(println)
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("org.bytedeco.javacpp.maxPhysicalBytes", "0")
    println("RuntimeConfig => " + RuntimeConfig())

//    val test = new Doc2vecSample
////    println("Count of Crawled => " + test.countAllData(199))
//    test.allData(199).take(20).foreach(println)

    createModel()
  }
}
