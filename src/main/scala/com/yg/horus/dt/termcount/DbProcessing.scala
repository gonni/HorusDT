package com.yg.horus.dt.termcount

import com.yg.horus.RuntimeConfig
import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

trait DbProcessing {
  val spark: SparkSession
  import spark.implicits._

  val prop = new Properties()
  prop.put("user", RuntimeConfig("mysql.user"))
  prop.put("password", RuntimeConfig("mysql.password"))

  def getCrawledData (seedNo: Long, limit: Int) = {
    val tableDf = spark.read.jdbc(RuntimeConfig("mysql.url"), "crawl_unit1", prop)

    tableDf.filter($"SEED_NO" === seedNo && $"STATUS" === "SUCC")
      .orderBy(desc("CRAWL_NO"))
      .select($"CRAWL_NO", $"ANCHOR_TEXT", $"PAGE_TEXT", $"PAGE_TITLE")
      .limit(limit)
  }


}
