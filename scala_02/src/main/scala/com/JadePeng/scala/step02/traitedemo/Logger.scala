package com.JadePeng.scala.step02.traitedemo

/**
  * 比如 trait 中可以包含很多子类都通用的方法，例如打印日志或其他工具方法等等。
  * spark就使用trait定义了通用的日志打印方法；
  */
trait Logger {
  /**
    * 定义一个具体的方法
    * @param message
    */
  def log(message:String) = println(message)

  //将订单数据写入本地服务器
  def writeBackendOrder()={
      println("写入数据库成功")
  }
}

class PersonForLog(val name:String) extends Logger{
  def makeFriends(other:PersonForLog) = {
  this.log(s"添加好友信息, 好友名称:${other.name}")
  }
}

object PersonForLog{
  def main(args: Array[String]): Unit = {
    val p1 = new PersonForLog("jack")
    val p2 = new PersonForLog("mike")

    p1.makeFriends(p2)
  }
}
