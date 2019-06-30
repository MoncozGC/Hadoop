package com.JadePeng.scala.step02

object MethodType {

  def getMiddle[T](canshu:T) ={
    canshu
  }

  def main(args: Array[String]): Unit = {
    // 从参数类型来推断类型
    println(getMiddle(Array("Bob", "had", "a", "little", "brother")).getClass.getTypeName)

    //指定类型，并保存为具体的函数。
    val f = getMiddle[String] _

    println(f("Bob"))
  }
}
