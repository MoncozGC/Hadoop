package com.JadePeng.scala.step02

/**
  * 泛型
  */
class GenericClass1 {
  private var context = 100

  def setContext(value: Int) = {
    context = value
  }
}

class GenericClass2 {
  private var context = ""

  def setContext(value: String) = {
    context = value
  }
}

/**
  * 定义一个范型类
  *
  * @tparam T
  */
class GenericClass[T] {
  private var context: T = _

  def setContext(value: T) = {
    context = value
  }
}

object GenericClass {
  def main(args: Array[String]): Unit = {
    val g1 = new GenericClass1
    g1.setContext(100)
    val g2 = new GenericClass2
    g2.setContext("hello")

    val g3 = new GenericClass[Int]
    g3.setContext(100)

    val g4 = new GenericClass[String]
    g4.setContext("hello")


  }
}

