package com.JadePeng.scala.step01

import scala.collection.mutable


/**
  * set数据集
  * 可变长
  * 不可变长
  *
  * 去重 无序
  *
  * @author Peng
  */
object SetDemo {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 定义一个不可变长的set集合
      */
    val set0 = Set(1, 2, 3, 4, 5)
    //set0(0) = 10 //不可改变

    /**
      * 2.可变长的set集合
      */
    val set2 = mutable.Set(1, 2)
    println(set2)
    //追加
    set2 += 2
    println(set2)
    //追加一个集合
    set2 ++= mutable.Set(3, 4, 5)
    println(set2)

    /**
      * set常用的方法
      */
    set2.size
    set2.head
    set2.last
    set2.max

    set2.map(_ * 2)
  }

}
