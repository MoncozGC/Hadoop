package com.JadePeng.scala.step01

import scala.collection.immutable

/**
  * For循环
  *
  * @author Peng
  */
object ForDemo {
  def main(args: Array[String]): Unit = {
    //左闭右和
    //使用to方法会产生一个连续不断的区间范围【0-5】
    println(" 左闭右和")
    for (i <- 0 to 5) print(i + "\t")


    //左边右开
    println("\n 左闭右开")
    for (i <- 0 until 5) print(i + "\t")

    //遍历字符串
    println("\n 遍历字符串")
    for (i <- "abcd") print(i + "\t")

    //将遍历过程中处理的结果返回到一个变量，使用yield关键字进行接收
    println("\n yield关键字")
    val for1: immutable.IndexedSeq[Int] = for (i <- 0 to 5) yield i
    println(for1)

    println("*****************九九乘法表****************************************")

    for (i <- 1 to 9) {
      for (j <- 1 to i) {
        if (i == j) {
          //控制行
          println(i + "*" + j + "=" + i * j)
        } else {
          //控制列
          print(i + "*" + j + "=" + i * j + "\t")
        }
      }
    }
    println("------------------多重循环--------------------------")
    for (i <- 0 to 9; j <- 1 to i) {
      if (i == j) {
        println(i + "*" + j + "=" + i * j)
      } else {
        print(i + "*" + j + "=" + i * j + "\t")
      }
    }

    //带有if守卫条件的for循环
    for (i <- 0 to 10; if (i % 2 == 0)) print(i+ "\t")
  }
}
