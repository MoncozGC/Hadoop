package com.JadePeng.scala.step02.casedemo

import scala.util.Random

//匹配字符串
object CaseDemo1 {
  def main(args: Array[String]): Unit = {
    //定义一个数组
    val arr = Array("hadoop", "zookeeper", "spark", "storm")

    //随机抽取数组中的一位，使用Random.nextInt
    val name = arr(Random.nextInt(arr.length))
    println(name)

    name match  {
      case "hadoop" => println("大数据分布式存储和计算框架")
      case "zookeeper" => println("大数据分布式协调服务框架")
      case "spark" => println("大数据分布式内存计算框架")
      case _ => println("找不到你")
    }

  }
}
