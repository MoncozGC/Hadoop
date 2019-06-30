package com.JadePeng.scala.step03

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

/**
  *
  * @author Peng
  */
class AActor(bActorRef: ActorRef) extends Actor {
  override def receive: Receive = {
    case "hadoop" => {
      println("HDFS")
      println("MapReduce")
      Thread.sleep(1000)
      //向BActor发送消息
      bActorRef ! "hadoop"
    }
  }
}

class BActor extends Actor {
  override def receive: Receive = {
    case "hadoop" => {
      println("DW")
      println("select")
      Thread.sleep(1000)
      //向AActor发送消息 sender可以获取到发送消息的ref
      sender() ! "hadoop"
    }
  }
}

object ActorGame extends App {
  //创建ActorSystem
  private val actorFactory = ActorSystem("actorFactory")
  //创建BActor应用代理
  private val bActorRef: ActorRef = actorFactory.actorOf(Props[BActor], "BActor")
  //创建AActor引用代理
  private val aActorRef: ActorRef = actorFactory.actorOf(Props(new AActor(bActorRef)), "AActor")

  //AActor
  aActorRef ! "hadoop"

}