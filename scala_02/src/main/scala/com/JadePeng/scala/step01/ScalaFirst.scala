package com.JadePeng.scala.step01

import scala.collection.mutable.ListBuffer

object ScalaFirst {
  def main(args: Array[String]): Unit = {
    print("hello scala")


    println("***************************************************")
    val list6 = new ListBuffer[String]
    list6.append("hello")
    list6.append("world")
    println(list6.mkString(","))
    val list7 = list6.toList
    println(list7)


    val function2 = list6.toList


  }
}
