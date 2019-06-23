package com.JadePeng.scala.step01

import scala.collection.mutable.ListBuffer

/**
  * List集合
  * 序列分为
  * 可变长
  * 不可边长: 长度不可变，角标元素也不能改变
  * 特点：底层是链表结构、插入有序、可以重复，查询慢
  *
  * @author Peng
  */
object ListDemo {
  def main(args: Array[String]): Unit = {

    //不可变长
    //1. 长度不可变，角标元素也不能改变
    var list01: List[Any] = List("hello", 7, 5)
    //list01(0)=100 //编译不报错运行报错
    println(list01)

    println(list01(0))

    //可重新赋值
    //2. 在尾部追加
    var list01cp1 = list01 :+ 50
    println(list01cp1)
    //3. 在头部追加
    var list01cp2 = 100 +: list01
    //连续添加只会添加最后一个
    list01cp2 = 200 +: list01
    println(list01cp2)

    //Nil：表示空的list，定义为list
    //nil：模式匹配的时候会用到
    //null、nothing、unit、 nil、none
    /**
      * 4.
      * :::运算符：三个冒号表示List的连接操作
      * ::两个冒号: 表示普通元素与List的连接操作
      *
      */
    val list4 = 1 :: 2 :: 3 :: list01 :: Nil
    println(list4)

    //5. 定义一个可变长序列
    val lb0 = ListBuffer(1, 2, 3)
    lb0.+=(4)
    lb0 += 5
    lb0 ++= List(4, 5, 6)
    lb0 ++= ListBuffer(4, 5, 6)
    lb0.append(7)

    /**
      * 6. list常用的方法
      */
    lb0.sum
    lb0.max
    lb0.min
    lb0.mkString(",")

    //7. 序列的转换操作
    println("序列转换 \t" + lb0.map(x => x))
    //8. filter过滤器
    println("过滤器 \t" + lb0.filter(x => x.equals(1)))


    //将lb0乘以3取偶数再排序
    val ex = lb0.map(x => x * 3).filter(x => (x % 2 == 0)).sorted
    println(ex)

    val reducer = lb0.reduce((x, y) => y)
    println(reducer)

  }
}
