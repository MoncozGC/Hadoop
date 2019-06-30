package com.JadePeng.scala.step02

/**
  * 实现一个单列对象
  */
class StaticTest {
   private def add(x:Int, y:Int) ={
    x+y
  }
}

//单例对象,静态对象
object StaticTest {
  val staticTest = new StaticTest
  println("这是一个单例对象")
  def add(x:Int, y:Int) ={
    staticTest.add(x, y)
  }
}

object Static{
  def main(args: Array[String]): Unit = {
    //第一个问题
//
//    val staticTest = new StaticTest
//    staticTest.add(2, 3)


    StaticTest.add(2, 3)
    StaticTest.add(3, 3)
    StaticTest.add(4, 3)
    StaticTest.add(5, 3)

  }
}