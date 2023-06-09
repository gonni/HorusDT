package com.yg.horus.dt.jobflow
import scala.collection.mutable.ListBuffer
import scala.util.Try

abstract class SerialJoblet(val period: Long, var remain: Long = 0) {
  var cntError = 0
  def delta(f:() => Unit) : Long= {
    val ts = System.currentTimeMillis()
    f()
    System.currentTimeMillis() - ts
  }

  def deltaRun(plusOffset: Long) = {
    if(remain - plusOffset < 0) {
      val timeEstimated = delta(run)
      remain = if (period - timeEstimated > 0) {
        val newPeriod = period - timeEstimated
        println("Set adapted estimated time : " + newPeriod)
        newPeriod
      }
      else {
        println("Invalid Estimated Time :" + timeEstimated)
        cntError += 1
        period * cntError
      }
//      remain = period
      println("New Remain = " + remain)
      timeEstimated
    } else {
      remain -= plusOffset
      println("Wait job, remain " + remain)
      0
    }
  }

  def run()

}

class SeiralJobManager(cntTurns: Int = 100, checkPeriod: Long = 5000L) {
  val jobs = ListBuffer[SerialJoblet]()

  def addJob(job: SerialJoblet) = {
    jobs += job
  }


  def start() = {
    var runEstimated = 0L
    for(i <- 1 to cntTurns) {
      println(s"Trun Start #${i} .. with delta: ${checkPeriod}")
      runEstimated = 0
      for(job <- jobs) {
        runEstimated += job.deltaRun(runEstimated + checkPeriod)
      }
      println("Turn Completed #" + i + " .. processed run time :" + runEstimated)
      sleepTime(checkPeriod - runEstimated)
    }
  }

  def sleepTime(time: Long = checkPeriod): Unit = {
    println(s"Sleep Turn for ${time}msec ..")
    if (time > 0) Thread.sleep(time)
    else println("No Sleep for this turn ..")
  }
}

object SeiralJobManager {
  def main(args: Array[String]): Unit = {
    println("Active System ..")


    val test = new SeiralJobManager()
    test.addJob(new SerialJoblet(30 * 1000, 100) {
      override def run(): Unit = {
        val s = Math.random() * (30 * 1000)
        println("RUN Joblet:50sec: " + s)
        Thread.sleep(s.toLong)
      }
    })

    println("=====================")
    test.start()
  }
}