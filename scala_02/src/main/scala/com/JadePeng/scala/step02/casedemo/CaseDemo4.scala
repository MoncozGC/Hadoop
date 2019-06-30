package com.JadePeng.scala.step02.casedemo

object CaseDemo4 {

  def main(args: Array[String]): Unit = {
    val arr = Array(0, 3, 5)

    arr match {
      case Array(1, x, y) => println(x + " " + y)
      case Array(1, _*) => println("1......")
      case Array(0) => println("only 0")
      case _ => println("something else")
    }

    val list0 = Nil //null 不分配内存
    val list2 = List() //分配内存，但是没数据

    //匹配序列
    val list = List(0)
    list match  {
      //case 0::Nil => println("only 0")
      case 0::tail => println("0.....")
      case x::y::z::Nil => println(s"x:${x}, y:${y}, z:${z}")
      case _ => println("something else")
    }
    println(list.head)
    println(list.tail)
    println(list.last)

    val tuple = (1, 3, 5)
    tuple match {
      case (1, x, y) => println(s"1, ${x}, ${y}")
      case (2, x, y) => println(s"2, ${x}, ${y}")
      case _ => println("others...")
    }
  }
}
