package com.JadePeng.scala.step03

import scala.actors.Actor

/**
  * @author Peng
  */
class Actor02  extends  Actor{
  override def act(): Unit = {
    receive{
      case  "start" => println("starting......")
      //  case _ => println("我没有匹配到任何消息")
    }
  }
}
object Actor02{
  def main(args: Array[String]): Unit = {
    val actor = new Actor02
    actor.start()
    actor ! "start"
  }
}
