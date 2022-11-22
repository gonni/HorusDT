package com.yg.horus
import com.typesafe.config.ConfigFactory

object RuntimeConfig {
  val conf = ConfigFactory.load("application.conf")
  var env: Option[String] = None

  def getRuntimeConfig() = {
    conf.getConfig(env.getOrElse(conf.getString("profile.active")))
//    conf.getConfig(conf.getString("profile.active"))
  }

  def setEnv(activeProfile: String) = {
    env = Some(activeProfile)
  }

  def getActiveProfile() = {
    conf.getString("profile.active")
  }
  def apply() = getRuntimeConfig()

  def apply(key: String) = getRuntimeConfig().getString(key)

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
