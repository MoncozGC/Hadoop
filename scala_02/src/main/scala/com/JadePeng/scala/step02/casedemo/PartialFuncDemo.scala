package com.JadePeng.scala.step02.casedemo

object PartialFuncDemo {

  def func2(x:String):Int={
    x match  {
      case "one"=> 1
      case "two"=> 2
      case _=> 0
    }
  }
//  ->: map定义的时候才能用，左边是key 右边是value
//  =>：函数定义或者模式匹配的时候

//  class -》 java class
//  object -》 java static class

  //偏函数
  val func1:PartialFunction[String, Int] ={
    case "one"=> 1
    case "two"=> 2
    case _=> 0
  }

  def main(args: Array[String]): Unit = {
    println(func1("two"))
    println(func1("one"))
    println(func1("abc"))
  }
}
