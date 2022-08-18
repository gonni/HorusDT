package com.yg.horus.dt.termcount

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.jdk.CollectionConverters.asScalaBufferConverter

trait NlpCommonUdf extends Serializable {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

  val getPlainTextUdf: UserDefinedFunction = udf[String, String] { sentence =>
    komoran.analyze(sentence).getPlainText
  }

  val getPlainTextUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    komoran.analyze(sentence).getPlainText.split("\\s")
  }

  val getNounsUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getNouns.asScala
  }

  val getTokenListUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getTokenList.asScala.map(x => x.toString)
  }

  val getTokenListUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    try {
      komoran.analyze(sentence).getTokenList.asScala.map(_.getMorph)
    } catch {
      case e: Exception => {
        println("Detected Null Pointer .. " + e.getMessage)
        Seq()
      }
    }
  }
}
