package com.JadePeng.scala.step02.casedemo

/**
  * 模式匹配的守卫：条件
  */
object CaseDemo2 {
  def main(args: Array[String]): Unit = {
    var ch = 50
    var sign = 0

    ch match {
//      case "+" => {
//        //操作数据库
//        sign = 1
//      }
//      case "-" => sign = 2
//      case "100" if ch.equals("100") => {sign = 5}
      case 100 if ch >= 100 => {sign = 5}
      case _ if ch.equals("500") => { sign =  3 }
      case _ if sign==3 => {sign = 4}
      case _ => println("无")//该条语句去掉会报错
    }
    println(ch + " " + sign)
  }
}
