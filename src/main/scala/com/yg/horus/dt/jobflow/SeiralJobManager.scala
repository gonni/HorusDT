package com.yg.horus.dt.jobflow
import scala.collection.mutable.ListBuffer

abstract class SerialJoblet(val period: Long, var remain: Long = 0) {
  def isRunCondition(offset: Long) = {
    if(remain - offset < 0) {
      remain = period
      true
    }
    false
  }

  def delta(f:() => Unit) : Long= {
    val ts = System.currentTimeMillis()
    f()
    System.currentTimeMillis() - ts
  }

  def deltaRun = delta(run)

  def deltaRun(offset: Long) = {
    if(remain - offset < 0L) {
//      println("run .. " + remain)
      remain = period
      delta(run)
    } else {
//      println("wait .. " + remain)
      remain = remain - offset
//      println("wait 2.. " + remain)
      0
    }
  }

  def run()

}

class SeiralJobManager(cntTurns: Int = 10, checkPeriod: Long = 5000L) {
  val jobs = ListBuffer[SerialJoblet]()

  def addJob(job: SerialJoblet) = {
    jobs += job
  }

  def start() = {
    var delta = 0L

    for(i <- 1 to cntTurns) {
      println(s"Run Trun #${i} ..")

      for(job <- jobs) {
        delta = job.deltaRun(1000 - delta)
//        println(delta)


      }
//      delta = 1000
    }
  }
}

object SeiralJobManager {
  def main(args: Array[String]): Unit = {
    println("Active System ..")


    val test = new SeiralJobManager()
    test.addJob(new SerialJoblet(5000) {
      override def run(): Unit = {
        val s = Math.random() * 1000
        println("5000 Run Test .. sleep: " + s)
        Thread.sleep(s.toLong)
      }
    })

    test.addJob(new SerialJoblet(1000) {
      override def run(): Unit = {
        val s = Math.random() * 1000
        println("1000 Run Test .. sleep: " + s)
        Thread.sleep(s.toLong)
      }
    })


    test.jobs.foreach(job => println("delta ->" + job.deltaRun(1000)))

    println("=====================")
    test.start()
  }
}