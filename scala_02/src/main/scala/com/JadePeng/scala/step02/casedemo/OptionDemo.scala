package com.JadePeng.scala.step02.casedemo

//Option: 有两个子类：Some(value)  None
object OptionDemo {
  def main(args: Array[String]): Unit = {
    val map = Map("a"-> 1, "b"-> 2)

    val value: Option[Int] = map.get("c")

    var result: String = ""
    value match {
      case Some(v) => {
        result = v.toString
        println("存在>"+v)
      }
      case None => println("不存在")
    }

    map.getOrElse("c", 100)

  }
}
