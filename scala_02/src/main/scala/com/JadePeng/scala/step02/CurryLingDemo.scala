package com.JadePeng.scala.step02

/**
  * 柯里化函数
  * 闭包
  */
object CurrylingDemo {

  /**
    * 普通的方法
    *
    * @param
    */
  def add(x: Int, y: Int) = {
    x + y
  }

  def add2(x: Int)(y: Int) = {
    x + y
  }

  def add3(x: Int)(y: Int)(z: Int) = {
    x + y
  }

  def main(args: Array[String]): Unit = {
    println(add(1, 2))
    println(add2(1)(2))
    println(add3(1)(2)(3))

    val z = 10

    //柯里化方法的推导过程，注意方法的返回值是一个函数体
    def keyrMethod(x: Int) = {
      //方法体：还是一个函数
      (y: Int) => {
        (x + y)
      }
    }

    //年终发奖金，技术部门每人15000奖金  销售部门每人10000的奖金
    val xiaoshou = keyrMethod(10000)
    val jishu = keyrMethod(15000)

    //发工资
    val zhangsan = xiaoshou(15000)
    println(zhangsan)
    val lisi = jishu(20000)
    println(lisi)
    val wangwu = xiaoshou(10000)
    println(wangwu)

    //注意：方法当中的函数，调用了方法的参数，就叫做闭包
    //子函数中调用了父函数或者外部变量的参数，就叫做闭包
  }
}
