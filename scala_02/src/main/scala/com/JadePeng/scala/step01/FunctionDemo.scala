package com.JadePeng.scala.step01

/**
  * 函数
  *
  * @author Peng
  */
object FunctionDemo {
  /**
    * 第一种形式
    * val 函数名:(参数名:参数类型)=>{ 函数体 }
    */

  val fun1 = (x: Int) => {
    x + 7
  }

  /**
    * 匿名函数:
    * (参数名:参数类型) => { 函数体 }
    */
  val fa = (x: Int, y: Int) => {
    x * y
  }

  /**
    * 实例三：
    * 第二种形式：
    * * val  函数名 ：（参数类型1，参数类型2） => (返回类型) = {
    * * 函数体
    * * }
    */
  val fun2: (Int) => (Int) = {
    x => x
  }

  val fun3: (String, Int) => (String, Int) = {
    (x, y) => {
      (x, y)
    }
  }

  def main(args: Array[String]): Unit = {
    //第一种形式
    println("第一种形式 \t" + fun1(23))
    //匿名函数
    println("匿名函数: \t" + fa(1, 2))

    //第二种形式
    val result = fun3("hello", 10)
    println("第二种形式 \t" + result)

    // 柯里化函数
    def ke(x: Int) = {
      (y: Int) => x + y
    }

    val xiaoShow = ke(10000)
    val jiShu = ke(15000)

    val zhangSan = xiaoShow(15000)
    println("柯里化函数 \t" + zhangSan)
  }
}
