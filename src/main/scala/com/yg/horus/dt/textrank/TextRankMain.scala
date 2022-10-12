package com.yg.horus.dt.textrank

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran

import scala.io.Source
import scala.annotation.tailrec
import scala.util.matching.Regex
import scala.math

import scala.jdk.CollectionConverters.asScalaBufferConverter

class Extractor {

  type Word = String
  type Sentence = List[String]

  implicit def arrayToList[T](array: Array[T])  = array.toList

  val stopList = loadStopList
  val threshold = 0.001
  val d = 0.85

  def loadStopList: List[String] =
    Source.fromFile("data/stopWord.txt").getLines.toList

  def isStop(target:String): Boolean = {
    @tailrec
    def recursion(low:Int, high:Int): Option[Int] = (low+high)/2 match{
      case _ if high < low => None
      case mid if stopList(mid) > target => recursion(low, mid-1)
      case mid if stopList(mid) < target => recursion(mid+1, high)
      case mid => Some(mid)
    }
    recursion(0, stopList.size - 1) != None
  }

  def loadText(fileName: String): String =
    if (fileName.length > 30) fileName.toLowerCase
    else Source.fromFile(fileName).mkString.toLowerCase

  def splitWords0(content: String): List[Word] =
    ("[a-zA-Z]+".r findAllIn content).toList

  def splitWords1(content: String): List[Word] = {
    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    komoran.setUserDic("/Users/a1000074/dev/myDic.txt")

    komoran.analyze(content).getTokenList.asScala.map(word => (word.getMorph + ":" + word.getPos)).toList
//    komoran.analyze(content).getNouns.asScala.toList
  }

  def splitWords(content: String): List[Word] = {
    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    komoran.setUserDic("/Users/a1000074/dev/myDic.txt")

//    komoran.analyze(content).getTokenList.asScala.filter(tk => {
//      tk.getPos.startsWith("NN") || tk.getPos.startsWith("VV") || tk.getPos.startsWith("VA")|| tk.getPos == "XR"
//    }).map(_.getMorph).toList
        komoran.analyze(content).getNouns.asScala.toList
  }

  def splitSentences(content: String): List[Sentence] =
    content split "[.?!-:]" map splitWords // May add more breakline signs

  def genHash[T](items: List[T]): Map[T, Int] =
    items.distinct.zipWithIndex.map(el => el._1 -> el._2).toMap

  def genInvHash[T](items: List[T]): Map[Int, T] =
    genHash(items).map(_.swap)

  def filterStop0(listOfWords: List[Word]): List[Word] = listOfWords filterNot isStop

  def filterStop(listOfWords: List[Word]): List[Word] = {
    listOfWords.filter(token => !stopList.contains(token))
  }

  def iterFunc(dis: Int => Map[Int, Double])
              (ld: List[Double], ths: Double): List[Double] = {

    def singleIter(neighbors: Map[Int, Double]): Double = {
      neighbors map {case (k, v) => v * ld(k) / dis(k).values.sum} sum
    }

    val newM = ld.zipWithIndex.map( el => (1 - d) + d * singleIter(dis(el._2)) )
    if ( (newM zip ld).map(t => math.abs(t._1 - t._2)).sum < threshold ) {
      ld
    }
    else newM
  }

  def textRank(maxIter: Int)
              (implicit f: (List[Double], Double) => List[Double],
               initM: List[Double]): List[Double] = {

    maxIter match {
      case 0 => initM
      case k => f(textRank(k - 1), threshold)
    }
  }
}

class KeywordExtractor(fileName: String) extends Extractor {

  val words = filterStop(splitWords(loadText(fileName)))
  val wordHash = genHash(words)
  val indexHash = genInvHash(words)
  val wordN = wordHash.size
  val dists = calcWordDist(7)
  override val threshold = 0.001

  def linkWord(a: Word, b: Word): List[(Int, Int)] = {
    List(wordHash(a) -> wordHash(b), wordHash(b) -> wordHash(a))
  }

  def linkWindow(window: List[Word]): List[(Int, Int)] = {
    window combinations(2) map(t => linkWord(t.head, t.last)) reduceLeft(_ ++ _)
  }

  def calcWordDist(windowSize: Int): Map[Int, List[Int]] = {
    val pairs = words sliding(windowSize) map(linkWindow) reduceLeft(_ ++ _)
    pairs.distinct groupBy(_._1) mapValues( _.map(_._2) )
  }

  def dis(idx: Int): Map[Int, Double] =
    dists getOrElse (idx, List(0)) map (_ -> 1.0) toMap

  def extract: List[(Double, String)] = {
    val a = textRank(50)(iterFunc(dis), List.fill(wordN)(1))
    a.zipWithIndex.map(t => (t._1, indexHash(t._2))).sortWith(_._1 > _._1)
  }
}

class SummaryExtractor(fileName: String) extends Extractor {

  val sentences = genListOfSentences(fileName)
  val sentenceHash = genHash(sentences)
  val indexHash = genInvHash(sentences)
  val sentencesN = indexHash.size
  val dists = calcSentDis(calcSimilarity)

  def genListOfSentences(fileName: String): List[Sentence] =
    splitSentences(loadText(fileName)) map filterStop map (_.distinct)

  def calcSimilarity(a: Sentence, b: Sentence): Double = {

    def timeCo(wd: Word): Int = if (a contains wd) 1 else 0

    def calcCo(leftSentece: Sentence): Int = leftSentece match {
      case word :: leftleft => timeCo(word) + calcCo(leftleft)
      case Nil => 0
    }

    if (a == b) 0
    else calcCo(b) / ((math log a.size) + (math log b.size))

  }

  def calcSentDis(f: (Sentence, Sentence) => Double): List[List[Double]] =
    sentences.map(t1 => sentences.map(t2 => f(t1, t2)))

  def dis(idx: Int): Map[Int, Double] =
    (dists apply idx).zipWithIndex map(_.swap) toMap

  def extract: List[(Double, Sentence)] = {
    val a = textRank(50)(iterFunc(dis), List.fill(sentencesN)(1))
    a.zipWithIndex.map(t => (t._1, indexHash(t._2))).sortWith(_._1 > _._1)
  }

}


object TextRankMain {
  def main(args: Array[String]): Unit = {
    println("Active System ..")
//    val ext = new Extractor
//    ext.loadStopList.foreach(println)
//
//    println("----------")
////    ext.filterStop(ext.splitWords(ext.loadText("data/sample_news.txt"))).foreach(println)
//    ext.filterStop(List("뉴스", "소울", "ABC", "기자")).foreach(println)


    val keyExt = new KeywordExtractor("data/sample_news.txt")
//    keyExt.splitWords1("언어 분석시 의미가 있는 따듯한 단어와 의미가 없는 단어나 조사 등이 달린다.").foreach(println)
//    keyExt.splitWords1("러시아의 우크라이나 침공을 공개적으로 비판한 러시아의 레전드 팝가수 알라 푸가체바(73)가 현재 이스라엘에 있다고 자신의 소식을 전했다").foreach(println)
    keyExt.extract.foreach(println)
  }
}
