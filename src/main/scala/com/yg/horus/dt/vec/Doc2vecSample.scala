package com.yg.horus.dt.vec

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import org.deeplearning4j.models.paragraphvectors._
import org.deeplearning4j.text.documentiterator.{LabelledDocument, SimpleLabelAwareIterator}
import org.deeplearning4j.text.tokenization.tokenizer.{TokenPreProcess, Tokenizer}
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.util
import scala.jdk.CollectionConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import kr.co.shineware.nlp.komoran.core.Komoran

class KomoranTokenFactory extends TokenizerFactory {

  val komoran = new Komoran(DEFAULT_MODEL.FULL)

  override def create(toTokenize: String): Tokenizer = new Tokenizer {

    val tokens = komoran.analyze(toTokenize).getTokenList.asScala
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

  def main(args: Array[String]): Unit = {
    val test = new Doc2vecSample
//    test.getData(100).foreach(println)

    val docs = test.getData(300).map(item => {
      val doc = new LabelledDocument
      doc.setContent(item.pageText.get)
      doc.setLabel("DOC_" + item.crawlNo)
      doc
    }).asJava

    docs.forEach(println)

    val iter = new SimpleLabelAwareIterator(docs)

    val tfac = new KomoranTokenFactory()
//    val iter = new DbLabelAwareIterator(1, 300)

    val vec = new ParagraphVectors.Builder()
      .minWordFrequency(5)
      .layerSize(200)
      .stopWords(new util.ArrayList[String]())
      .windowSize(7)
      .iterate(iter)
      .tokenizerFactory(tfac)
      .build()

    vec.fit()

    println("--------------------<B>-------------------")
    vec.wordsNearest("월드컵", 10).forEach(println)

    println("--------------------<A>-------------------")
    vec.nearestLabels("월드컵", 10).forEach(println)


//    val test = tkFac.create("순이익금을 유보하는 미처분 이익잉여금 철저하게 관리해야 하는 " +
//      "이익잉여금대전에서 제조업을 하는 J기업의 문 대표는 경리담당 직원을 통해 주기적으로 통장 잔고를 확인하고 있었습니다.")
//    while(test.hasMoreTokens) {
//      println("->" + test.nextToken())
//    }
  }
}
