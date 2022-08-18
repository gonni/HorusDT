package com.yg.horus
import com.typesafe.config.ConfigFactory

object RuntimeConfig {
  val conf = ConfigFactory.load()

  def getRuntimeConfig() = {
    val profile = System.getProperty("active.profile")
//    println("active profile :" + profile)
    if(profile != null) conf.getConfig(profile) else conf.getConfig("office_local")
//    conf.getConfig("office_desk")
  }

  def getActiveProfile() = {
    conf.getString("profile.active")
  }

  def main(args: Array[String]): Unit = {
    println("Active System ..")
//    println("=>" + getClass.getClassLoader.getResource("myDic.txt").getPath)
    val rConf = conf.getConfig("office_desk")

    println("Active Profile =>" + rConf.getString("profile.name"))

    println("Active Profile2 =>" + getRuntimeConfig())
  }

}
