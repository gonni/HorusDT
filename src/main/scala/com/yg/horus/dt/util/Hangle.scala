package com.yg.horus.dt.util

import com.yg.horus.RuntimeConfig
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran

object Hangle {
  def main(args: Array[String]): Unit = {
    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
//    val dic = "/Users/ygkim/dev/nnp_dic.txt"
//    println(">>>" + RuntimeConfig("komoran.dic"))
//    komoran.setUserDic(dic)
    komoran.setUserDic(RuntimeConfig("komoran.dic"))

    val src = "대통령 윤석렬은 유지할 수 있을까? 대통령실 영부인 김건희는 사기꾼이 확실한데 .. 윤 대통령을 국민의 힘 국민의힘이 지켜줄라나"
    komoran.analyze(src).getNouns.forEach(println)


  }
}
