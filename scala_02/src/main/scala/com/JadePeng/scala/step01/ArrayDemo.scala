package com.JadePeng.scala.step01

import scala.collection.mutable.ArrayBuffer

/**
  * 数组
  * 可变数据 and 不可变数据
  *
  * @author Peng
  */
object ArrayDemo {
  def main(args: Array[String]): Unit = {
    /**
      * 不可变长的数组
      * 不可变长数组一旦被初始化，数组的长度不可变，内容可变
      * 如果对象初始化的时候不需要new，类中一定要实现apply方法
      * 如果不加new关键字，那么就需要传递初始化的一堆参数
      * 如果加上new关键字，那么不需要初始化，给定一个长度，告诉数组最长可以有多少个元素
      */
    var arr01: Array[Int] = Array[Int](1, 2, 3, 4, 5)
    //定长数组转换成变长数组 toBuffer
    println("arr01 \t" + arr01.toBuffer)
    //不可变长数组，长度不可变，内容可变
    arr01(0) = 100
    println("arr01 \t" + arr01.toBuffer)

    val arr02 = Array.apply(1, 2, 3, 4, 5)
    println("arr02 \t" + arr02.toBuffer)


    //将两个数组的数据进行合并
    val arr04 = arr01 ++ arr02
    println("arr04 \r" + arr04.toBuffer)

    //在Scala中数组中的类型可不一致, 遍历数组
    val arr03: Array[Any] = Array(1, 2, 3, 4, "hello")
    print("arr03 \t")
    for (i <- arr03) {
      print(i)
    }

    /**
      * 可变长数组
      * 如果实现可变长数组的话，需要显式的导入： scala.collection.mutable.ArrayBuffer
      * 再scala中默认的都是不可变集合
      *
      * +=：一次添加一个元素
      * -=：一次删除一个元素
      * ++=：一次添加一个同一个类型的集合
      */
    val arrBuffer = ArrayBuffer[Int]()

    //添加一个元素
    arrBuffer.+=(1)
    arrBuffer += 4
    //一次添加多个元素
    arrBuffer += (5, 6, 7)

    //一次加一个集合
    arrBuffer ++= Array(8, 9) //添加一个不可变集合
    arrBuffer ++= ArrayBuffer(10, 11) //添加一个可变集合

    //修改一条数据
    arrBuffer(1) = 100
    arrBuffer.append(10, 20)

    //删除一条数据
    arrBuffer.-=(100)
    arrBuffer.-=(1, 2, 3)
    arrBuffer --= Array(1, 2, 3, 4)
    arrBuffer.remove(0) //删除角标
    arrBuffer.remove(0, 2) //从指定角标的位置开始删除，删除2条数据

    //将定长数据转换成可变长数组
    println(arr01.toBuffer) //当不可变长数组输出的时候需要转换成buffer
    println(arrBuffer) //

    //将一个可变长数组，转换成定长数组
    arrBuffer.toArray

    println("****************************************")

    //多维数组
    //定义一个三行四列的多维数组
    val dim: Array[Array[Double]] = Array.ofDim[Double](3, 4)
    dim(1)(1) = 1.1
    for (i <- dim) {
      println(i.toBuffer)
    }

    /**
      * 数组常用的方法
      */
    println("arr01最大值 \t" + arr01.max)
    println("arr01最小值 \t" + arr01.min)
    println("arr01的和 \t" + arr01.sum)
    println("****************************************")

    println(arrBuffer)
    //分隔
    println(arrBuffer.mkString(","))
    //拼接
    println(arrBuffer.mkString("[", ",", "]"))

    println("****************************************")

    /**
      * 数组的常用转换操作
      */
    val f1 = (x: Int) => x * 10
    val arrayBuffer2: ArrayBuffer[Int] = arrBuffer.map(f1)
    arrBuffer.map(_ * 2)

    val strings: ArrayBuffer[Int] = arrBuffer.map(x => {
      x
    })

    for (i <- arrBuffer) {
      println(i)
    }
  }
}
