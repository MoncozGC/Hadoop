package com.JadePeng.scala.step03

import akka.actor.{Actor, ActorSystem, Props}

/**
  *
  * @author Peng
  */
//当我们继承actor以后，就是一个Actor，核心方法是receive
class SayHelloActor extends Actor {

  //1. receive方法, 会被该Actor的mailBox(实现了Runnable接口的类)调用
  //2. 当该Actor的mailBox接收到消息以后,就会调用receive
  //3. type Receive = PartialFunction[Any,Unit], 因为是异步的所有第二个没有返回值
  override def receive: Receive = {
    case "hello" => println("你好")
    case "ing" => println("一起吧")
    case "error" => println("绝交!")
    case "exit" => {
      println("我要走了")
      //停止ActorRef
      context.stop(self)
      //退出ActorSystem
      context.system.terminate()
    }
    case _ => println("我不认识你")
  }
}

//object是一个单例类 而我们的leader(ActorSystem)只需要一个
object SayHelloActorDemo {
  //1. 创建一个ActorSystem, 专门用于创建Actor
  private val actorFactory = ActorSystem("actorFactory")
  //2. 创建一个Actor的同时返回Actor的ActorRef
  private val sayHelloActorRef = actorFactory.actorOf(Props(new SayHelloActor), "sqyHelloActor")

  def main(args: Array[String]): Unit = {
    //给sayHelloActor发送消息（邮件）
    sayHelloActorRef ! "hello"
    sayHelloActorRef ! "ing"
    sayHelloActorRef ! "error"
    sayHelloActorRef ! "exit"
  }
}
