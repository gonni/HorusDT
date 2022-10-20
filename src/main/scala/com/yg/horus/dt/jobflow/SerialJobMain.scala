package com.yg.horus.dt.jobflow

class LdaJob(period: Long) extends SerialJoblet(period) {

  override def run(): Unit = {
    println("Sleep .. 1")
    Thread.sleep(1013L)
    println("Sleep .. 2")
  }

}

object SerialJobMain {

  def main(args: Array[String]): Unit = {
    println("Active Serial Job ..")
    implicit def now : Long = System.currentTimeMillis()

    val ldaJob = new LdaJob(10)
    println("Delta => " + ldaJob.deltaRun)
  }
}
