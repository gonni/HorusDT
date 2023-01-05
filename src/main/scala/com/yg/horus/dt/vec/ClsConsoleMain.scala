//package com.yg.horus.dt.vec
//
//import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
//import org.deeplearning4j.models.paragraphvectors.ParagraphVectors
//
//import scala.io.StdIn.readLine
//import scala.collection.JavaConverters._
//
//object ClsConsoleMain {
//
//  def parseCommand(cmd: String) = {
//    val tokens = cmd.split("\\|")
//
//    val positives = tokens.filter(_.startsWith("+")).map(_.substring(1))
//    //    positives.foreach(println)
//    val negatives = tokens.filter(_.startsWith("-")).map(_.substring(1))
//    //    negatives.foreach(println)
//
//    (positives, negatives)
//  }
//
//  def runCls(vec : ParagraphVectors) = {
//    System.setProperty("org.bytedeco.javacpp.maxPhysicalBytes", "0")
//    println("Active Command Line Service ..")
//
//
//    var isRunning = true
//    var cmd = ""
//    while (isRunning) {
//      print("Input Command(ex: +무협|-분노의 질주|-소설|+강호) :")
//      cmd = readLine()
//
//      if (cmd.equals("quit")) {
//        isRunning = false
//      } else {
//        println("Command input is " + cmd)
//
//        val posNeg = parseCommand(cmd)
//        println("Near Words Top10 ---------------------------------")
//        vec.wordsNearest(posNeg._1.toList.asJava, posNeg._2.toList.asJava, 10).forEach(println)
//        println("Near Words Top10 ---------------------------------")
//        vec.nearestLabels(posNeg._1.mkString(" "), 10).forEach(println)
//      }
//
//      println
//    }
//
//    println("CLS finished ..")
//  }
//
//  def main(args: Array[String]): Unit = {
////    val cmd = "+무협|-분노의 질주|-소설|+강호"
////    parseCommand(cmd)._2.foreach(println)
//    System.setProperty("org.bytedeco.javacpp.maxPhysicalBytes", "0")
//    val korTokenizerFac = new KomoranTokenFactory()
//    val vec = WordVectorSerializer.readParagraphVectors("./story_prod_all.mdl")
//    vec.setTokenizerFactory(korTokenizerFac)
//    println("Load Model Completed, Ready ..")
//
//    runCls(vec)
//  }
//}
