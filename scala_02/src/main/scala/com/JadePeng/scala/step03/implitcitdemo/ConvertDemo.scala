package com.JadePeng.scala.step03.implitcitdemo

/**
  * 隐式转换作用：
  * 代码更加简洁，让我们的程序更加优雅
  */
object ConvertDemo {

  //写一个隐式转换方法，将double类型转换成int类型（悄悄的转换的）
  implicit def double2Int(num:Double):Int = { num.toInt }

  def main(args: Array[String]): Unit = {
    val num:Int = 3.5
  }
}
