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

  def deltaRun2(plusOffset: Long) = {
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

//  def deltaRun(offset: Long) = {
//    if(remain - offset < 0L) {
//      remain = period
//      delta(run)
//    } else {
//      remain = remain - offset
//      0
//    }
//  }

  def run()

}

class SeiralJobManager(cntTurns: Int = 100, checkPeriod: Long = 5000L) {
  val jobs = ListBuffer[SerialJoblet]()

  def addJob(job: SerialJoblet) = {
    jobs += job
  }


  def start2() = {
    var runEstimated = 0L
    for(i <- 1 to cntTurns) {
      println(s"Trun Start #${i} .. with delta: ${checkPeriod}")
      runEstimated = 0
      for(job <- jobs) {
        runEstimated += job.deltaRun2(runEstimated + checkPeriod)
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

//  def start() = {
//    var delta = 0L
//    var isDelayed = false ;
//    for(i <- 1 to cntTurns) {
//      println(s"Trun Start #${i} .. with delta: ${delta}")
//      if(delta > checkPeriod) isDelayed = true else isDelayed = false
//
//      for(job <- jobs) {
//
//        delta += job.deltaRun(delta)
////        println(i + " : plus detla :" + delta)
//      }
//      println("Turn Completed #" + i + " .. processed time :" + delta)
//      if(isDelayed) delta = checkPeriod
//
//      delta = sleepDelta(delta)
//      println
//    }
//  }



//  def sleepDelta(offset: Long = 5000L) = {
//    val spTime = checkPeriod - offset
//    try {
//      if(spTime > 0) {
//        println("JOB Period Sleep : " + spTime)
//        Thread.sleep(spTime)
//      } else {
//        println("JOB Period No Sleep : " + spTime)
//      }
//    }
//    catch {
//      case e: Exception => println(s"Detected error in sleep : ${e.getMessage}")
//    }
//
//    if(spTime > 0) checkPeriod
//    else offset
//  }
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

//    test.addJob(new SerialJoblet(30 * 1000) {
//      override def run(): Unit = {
//        val s = Math.random() * 20000
//        println("RUN Joblet:30sec .. sleep: " + s)
//        Thread.sleep(20000)
//      }
//    })


//    test.jobs.foreach(job => println("delta ->" + job.deltaRun(1000)))

    println("=====================")
    test.start2()
  }
}