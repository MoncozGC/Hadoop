package com.JadePeng.scala.step02

/**
  * 枚举类型的使用
  * 数据库中有个表这个表中有所有用户的操作行为，肯定有个字段记录了操作类型：int：0，1，2
  * 将来程序中根据用户行为类型进行获取数据的话
  * 0 == OperationType.View.id：查看操作
  * 1 == OperationType.View.id：submit操作
  *
  * 0 == EnumDemo.view
  */
object OperationType extends Enumeration {
  type OperationType = Value
  val View = Value(10, "view")
  val Submit = Value(1, "submit")
  val Del = Value(2, "del")

  //在一定程度上，字符串可以替代枚举
  val view = 0

  def main(args: Array[String]): Unit = {
    println(OperationType.View)
    println(OperationType.View.id)
    println(OperationType.view)
  }

}
