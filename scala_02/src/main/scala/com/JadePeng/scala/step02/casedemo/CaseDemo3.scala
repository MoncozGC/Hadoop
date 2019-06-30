package com.JadePeng.scala.step02.casedemo

import scala.util.Random

object CaseDemo3 {
  def main(args: Array[String]): Unit = {
    val arr = Array("hello", 1, 2.0, CaseDemo3)

    //随机获取数组中的元素
    val value = arr(Random.nextInt(arr.length))

    println(value)

    value match {
      case x: Int => println("Int=>" + x)
      case y: Double if (y >= 0) => println("Double=>" + y)
      case h: Double if (h > 1) => println("h-Double=>" + h)
      case z: String => println("String=" + z)
      case _ => throw new Exception("not match exception")
    }
  }
}
