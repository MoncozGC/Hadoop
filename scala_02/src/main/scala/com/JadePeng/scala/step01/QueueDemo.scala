package com.JadePeng.scala.step01

import scala.collection.{immutable, mutable}

/**
  *队列
  *
  * @author Peng
  */
object QueueDemo {
  def main(args: Array[String]): Unit = {
    //创建不可变的队列
    val queue1 = immutable.Queue[Int](1,2,3)
    //创建可变的队列
    val queue2 = mutable.Queue[Int](1,2,3)

    //增加元素,不可变队列，不可以改变其长度
    //queue1 .+= (1) 报错

    //queue1(0) = 10
    queue2 .+= (1)

    //按照队列删除队列的元素，因为先进先出原则FIFO，所以删除数据的话先删除第一个元素
    val dequeue  = queue1.dequeue

    queue2.enqueue(4,5,6)

    queue2.head
    queue2.last
  }
}

