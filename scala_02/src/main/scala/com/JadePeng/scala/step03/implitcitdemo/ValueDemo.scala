package com.JadePeng.scala.step03.implitcitdemo

object ValueDemo {

  //隐式参数必须要跟方法中的参数类型保持一致
  implicit val abc = 10
  //定义多个隐式参数，语法没有错误，但是有编译错误
  //implicit val bcd = 20

  def test()(implicit  i:Int = 100) ={
    println(i)
  }

  def main(args: Array[String]): Unit = {
    //隐式参数的优先级问题
    //方法在执行时，发现一个需要隐式的int类型的参数，就会在方法执行的环境中（程序的上下文中）
    //查找跟隐式变量类型一致的参数，如果有就使用，没有就使用默认值，如果还没有默认值，就报错
    //优先级：传入的>上下文定义的类型一致的隐式值>默认值
    test()(1000)
  }
}
