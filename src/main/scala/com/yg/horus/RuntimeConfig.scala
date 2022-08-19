package com.yg.horus
import com.typesafe.config.ConfigFactory

object RuntimeConfig {
  val conf = ConfigFactory.load()

  def getRuntimeConfig() = {
    val profile = System.getProperty("profile.active")
    if(profile != null) conf.getConfig(profile) else conf.getConfig(conf.getString("profile.active"))
  }

  def getActiveProfile() = {
    conf.getString("profile.active")
  }

  def main(args: Array[String]): Unit = {
    println("Active System ..")
//    println("=>" + getClass.getClassLoader.getResource("myDic.txt").getPath)
//    val rConf = conf.getConfig("office_desk")
//
//    println("Active Profile =>" + rConf.getString("profile.name"))
//
//    println("Active Profile2 =>" + getRuntimeConfig())

    val aConf = conf.getString("profile.active")
    println("Conf -> " + aConf)

  }

}
