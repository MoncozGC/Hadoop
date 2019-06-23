package com.JadePeng.scala.step01

/**
  * 懒加载
  * 使用场景
  * 主要是用于初始化开销比较大的场景：
  * 创建mysql数据库连接，当程序初始化的时候不执行mysql连接创建，而是再使用数据库的时候进行创建
  * 这样的话可以大大的提高程序初始化的速度
  *
  * @author Peng
  */
object LazyDemo {

  def init() = {
    println("北京欢迎您")
    "懒加载"
  }

  def main(args: Array[String]): Unit = {
    lazy val msg = init()
    println("Lazy方法执行")
    println(msg)
  }
}
