package com.JadePeng.scala.step01

/**
  * 元组对象
  * 元祖对象最大支持22个元素，如果只有两个元素，通常称作 对偶元祖
  * 元祖对象角标从1开始，最长22
  *
  * @author Peng
  */
object TupleDemo {
  def main(args: Array[String]): Unit = {
    //定义一个元组对象
    val tuple1 = ("hello", 1, 0f)
    println(tuple1)
    val tuple2 = (1)
    println(tuple2)

    //unary 一元的
    val result = tuple2.unary_+
    println(result)

    //遍历元组对象
    println("遍历元组对象: ")
    for (x <- tuple1.productIterator) {
      println("\t" + x)
    }

    //拉链操作
    val arr1 = Array("a", "b", "c", "d")
    val arr2 = Array("A", "B", "C")

    val tuple = arr1.zip(arr2)
    println(tuple.toBuffer)
  }
}
