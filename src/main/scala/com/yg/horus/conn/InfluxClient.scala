package com.yg.horus.conn

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Source}
import com.influxdb.annotations.{Column, Measurement}
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.scala.InfluxDBClientScalaFactory
import com.influxdb.client.write.Point
import com.yg.horus.RuntimeConfig

import java.time.Instant
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object InfluxClient extends Serializable {
  implicit val system: ActorSystem = ActorSystem("examples")

//  val token = "5nWBmnhyUFbfF3q3F_yAfr4Wklis0HQT0UFKU2qf3z29bbsGMjPxYBeP34oz__byN8aSmS4hYud2zlR8tewDrA=="
//  val org = "NA"
//  val token = "CwgQWYIZKOcSpdlxwpfZfvDWQXpsfTlt7o2GD5hFAs4rTvHDF-7cfwmIQnmdocqL__5uoabCFGuf_GYzFQfxIA=="
//  val org = "xwaves"

  val conf = RuntimeConfig.getRuntimeConfig()
  val url = conf.getString("influx.url")
  val token = conf.getString("influx.authToken")
  val org = conf.getString("influx.org")
  val bucket = "tfStudySample"

  val client = InfluxDBClientScalaFactory.create(url, token.toCharArray, org, bucket)

  def fluxQuery(fluxQuery : String) = {
    val result = client.getQueryScalaApi().query(fluxQuery)

    var conv = Seq[TermCount]()
    Await.result(result.runForeach(f => {
      conv = conv :+ TermCount(f.getValueByKey("term").toString, f.getValue.toString.toLong)
    }), Duration.Inf)

    conv.sortBy(-_.count).take(10).foreach(println)

  }

  case class TermCount(term: String, count: Long)

  def fluxQuery2(queryStr: String) = {

  }

  def main(args: Array[String]): Unit = {
    val query =
      """
        |from(bucket:"tfStudySample")
        ||> range(start: -3h)
        ||> filter(fn: (r) => r._measurement == "term_tf" and r._field == "tf")
        ||> sum()
        ||> filter(fn: (r) => r._value > 0)
        ||> sort(columns: ["_value"])
        ||> limit(n: 3)
        |""".stripMargin

    fluxQuery(query)

    //
    // Use a Data Point to write data
    //
//    val point = Point
//      .measurement("mem")
//      .addTag("host", "host1")
//      .addField("used_percent", 3.43234543)
//      .time(Instant.now(), WritePrecision.NS)
//
//    val sourcePoint = Source.single(point)
//    val sinkPoint = client.getWriteScalaApi.writePoint()
//    val materializedPoint = sourcePoint.toMat(sinkPoint)(Keep.right)
//    Await.result(materializedPoint.run(), Duration.Inf)
//
//    println("Successfully completed ..")
//
//    //
//    // Use POJO and corresponding class to write data
//    //
//    val mem = new Mem()
//    mem.host = "host1"
//    mem.used_percent = 22.43234543
//    mem.time = Instant.now
//
//    val sourcePOJO = Source.single(mem)
//    val sinkPOJO = client.getWriteScalaApi.writeMeasurement()
//    val materializedPOJO = sourcePOJO.toMat(sinkPOJO)(Keep.right)
//    Await.result(materializedPOJO.run(), Duration.Inf)

    client.close()
    system.terminate()
  }

  def shutdownClient(): Unit = {
    client.close()
    system.terminate()
  }

  def writeTf(seedId: Long, term: String, count: Int) : Unit = {
//    println(s"write ${term}: ${count} to influx")
    val point = Point
      .measurement("term_tf")
      .addTag("seedId", seedId.toString)
      .addTag("term", term)
      .addField("tf", count)
      .time(Instant.now(), WritePrecision.NS)

    val sourcePoint = Source.single(point)
    val sinkPoint = client.getWriteScalaApi.writePoint()
    val materializedPoint = sourcePoint.toMat(sinkPoint)(Keep.right)
    Await.result(materializedPoint.run(), Duration.Inf)

//    val termTf = new TermTf
//    termTf.term = term
//    termTf.tf = count
//    termTf.time = Instant.now
//
//    val sourcePOJO = Source.single()
//    val sinkPOJO = client.getWriteScalaApi.writeMeasurement()
//    val materializedPOJO = sourcePOJO.toMat(sinkPOJO)(Keep.right)
//    Await.result(materializedPOJO.run(), Duration.Inf)
  }

}

@Measurement(name = "mem")
class TermTf {
  @Column(tag = true)
  var term: String = _
  @Column(tag = true)
  var seedId: Long = _
  @Column
  var tf: Int = _
  @Column(timestamp = true)
  var time: Instant = _
}

@Measurement(name = "mem")
class Mem() {
  @Column(tag = true)
  var host: String = _
  @Column
  var used_percent: Double = _
  @Column(timestamp = true)
  var time: Instant = _
}
