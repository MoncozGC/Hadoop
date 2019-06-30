package com.JadePeng.scala.step03

import scala.actors.Actor

/**
  *
  * @author Peng
  */
class Actor1 extends Actor{
  override def act(): Unit = {
    for (i <- 1 to 5){
      println("actor1 ===== " + i)
    }
  }
}

object Actor2 extends Actor{
  override def act(): Unit = {
    for (j <- 1 to 10){
      println("actor2 =====" + j)
    }
  }
}

object Actor1{
  def main(args: Array[String]): Unit = {
    val actor = new Actor1
    actor.act()
    Actor2.act()
  }
}
