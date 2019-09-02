package com.JadePeng.scala.step01

/**
  * 方法和函数的区别
  * 再函数式编程中，函数是一等公民，可以想任何其他数据类型一样被传递和操作
  */
object MethodAndFunctionDemo {

  //普通的方法
  def m1(x : String) ={
    x
  }

  //这是一个要求传递函数的方法
  def m2(fun : (Int, Int)=>Int) ={
      fun(2, 6)
  }

  def m3(x:Int) ={ x * x }

  def m4(x:Int => Int):Int ={
    x(100)
  }

  //定义一个函数f1，参数是两个int类型，返回值是一个int类型
  val f1 = (x:Int, y:Int) => x+y

  val f2 = (x:Int, y:Int) => x*y

  def main(args: Array[String]): Unit = {
      //调用m2方法，传入f1,将函数作为参数传递到方法中，该函数（方法）就变成了高阶函数
    val mf1 = m2(f1)
    println(mf1)

    val mf2 = m2(f2)
    println(mf2)

    //调用操作
    val mf3 = m4(m3 _)//程序会默认的将m3转换成函数，m3本来是一个方法，是再m3  后面偷偷的加上了一个下划线

    //赋值操作
    val mf4 = m4 _  //报错，因为这时候没有人给你偷偷加下划线
  }
}
