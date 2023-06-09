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
  implicit val system: ActorSystem = ActorSystem("InfluxActor")

  val conf = RuntimeConfig.getRuntimeConfig()
  val url = conf.getString("influx.url")
  val token = conf.getString("influx.authToken")
  val org = conf.getString("influx.org")
  val bucket = "tfStudySample"

  println("url -> " + url)
  println("token -> " + token)
  println("org -> " + org)


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

    client.close()
    system.terminate()
  }

  def shutdownClient(): Unit = {
    client.close()
    system.terminate()
  }

  def writeTf(seedId: Long, term: String, count: Int) : Unit = {

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
