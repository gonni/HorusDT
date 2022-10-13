package com.yg.nsp
import scala.collection.mutable
import scala.io.Source
import java.io._

object CsvConvert {
  case class CsvUnit(prodId: String, prodName: String, desc: String, artist: String)

  def main(args: Array[String]): Unit = {
    println("Active System..")

    val allTokens = Source.fromFile("/Users/a1000074/Documents/book_desc_6m.csv").getLines()
    val sb = new StringBuilder()
    val res = mutable.MutableList[CsvUnit]()
    var first: Boolean = true

    allTokens.foreach(line => {
      if(first) {
        first = false
      } else {
        if (line.startsWith("H0")) {

          val tokenSeq = sb.toString().split("<,>").toSeq
          if (tokenSeq.length == 4) {
//            println("->" + CsvUnit(tokenSeq(0), tokenSeq(1), tokenSeq(2), tokenSeq(3)))
            res += CsvUnit(tokenSeq(0), tokenSeq(1), tokenSeq(2), tokenSeq(3))
          } else
            println("Invalid :" + sb)
          sb.clear()
        }
        sb ++= (line + " ")
      }
    })

    res.foreach(println)

    craeteCsvFile(new File("sample1.csv"), res, "<|>")
  }

  def craeteCsvFile(file: File, data: mutable.MutableList[CsvUnit], sep: String) = {
    val pw = new PrintWriter(file)
    data.foreach(cu => {
      pw.println(cu.prodId + sep + cu.prodName + sep + cu.desc + sep + cu.artist)
    })
    pw.flush
    pw.close
  }

}
