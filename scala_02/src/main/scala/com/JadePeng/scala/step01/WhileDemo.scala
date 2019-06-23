package com.JadePeng.scala.step01

/**
  *
  * @author Peng
  */
object WhileDemo {
  def main(args: Array[String]): Unit = {
    //1. do while
    var num = 0
    do {
      //num++
      //println("num="+num)
      println(s"num=$num")
      num = num + 1
    }
    while (num <= 5)

    //2. while
    num = 0
    println(s"-----------------------------------------")
    while (num <= 5) {
      println(s"num=$num")
      num = num + 1
    }

    //3. while 返回值
    println("------------------------------------")
    num = 0
    var num2 = 0
    //while循环可以有返回值
    var num1 = while (num <= 5) {
      num2 = num2 + 1
      num = num + 1
    }
    println(num1)
    println(num2)

    println("------------跳出while------------------------")

    var num3 = 0
    //下滑线是导入这个包下的所有类，类似于java中的*
    //两种方式跳出for循环，一种是使用brakerable，另外一种使用标记，还有一种函数
    import scala.util.control.Breaks._
    breakable({
      while (true) {
        num3 = num3 + 1
        println(num3)
        if (num3 > 5) {
          break()
        }
      }
    })

    println("-------------跳出while-----------------------")

    num3 = 0
    var flag = true
    while (flag) {
      num3 = num3 + 1
      if (num3 > 5) {
        flag = false
      }
      println(num3)
    }

    println("end")
  }
}
