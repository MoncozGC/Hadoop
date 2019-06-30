package com.JadePeng.scala.step02.casedemo

import scala.util.Random

//样例类
//1：自动实现了序列化
//2：使用的时候不需要new
//3：可以进行模式匹配
case class SubmitTask(id:String, name:String)
case class HeartBeat(time:Long)
case object CheckTimeOutTask
case object StopTask

object CaseDemo5 {
  def main(args: Array[String]): Unit = {
    //创建一个数组
    val arr = Array(CheckTimeOutTask, HeartBeat(1000), SubmitTask("001", "task_001"), StopTask)

    val value = arr(Random.nextInt(arr.length))

    value  match {
      case SubmitTask(id, username) => println(s"id:${id}, name:${username}")
      case HeartBeat(time) => println(s"time:${time}")
      case CheckTimeOutTask => println("check...")
      case StopTask => println("stop...")
    }
  }
}
