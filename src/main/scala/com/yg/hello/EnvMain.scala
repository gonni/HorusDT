package com.yg.hello

import com.yg.horus.RuntimeConfig

object EnvMain {
  def main(args: Array[String]): Unit = {
    println("Active ..")

    if(args.length > 0) {
      println(s"Args : {}", args(0))
      System.setProperty("active.profile", args(0))
    } else {
      println("NoArgs ..")
      System.setProperty("active.profile", "office_desk")
    }

    println("Valid Config" + RuntimeConfig.getRuntimeConfig())
  }
}
