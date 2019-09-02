package com.JadePeng.scala.step03.sparkdemo

import java.util.UUID
import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Worker(masterHost:String,masterPort:Int, workerHost:String, workerPort:Int, cores:Int, memory:Int) extends  Actor{
  //work创建以后，要跟master建立连接，每个worker节点可以生成一个编号
  val workerid = UUID.randomUUID().toString
  //master节点的代理
  var masterRef: ActorSelection = null
  /**
    * worker主构造器执行以后，recive执行之前会执行preStart
    */
  override def preStart(): Unit = {
    //连接master，设置master的地址和端口号
    masterRef = context.actorSelection(s"akka.tcp://${Master.MASTER_SYSTEM}@${masterHost}:${masterPort}/user/${Master.MASTER_NAME}")
    //向master发送消息
    masterRef ! RegisterWorkInfo(workerid, cores, memory)
  }
  override def receive: Receive = {
    //master节点向worker节点回复注册成功的消息
    case RegisterWorker =>{
      val actorRef: ActorRef = self
      import context.dispatcher
        //启动一个定时器，定时向master节点发送心跳信息
      context.system.scheduler.schedule(0 millis, 5000 millis, actorRef, SendHearheat)
    }
      //自己给自己发送消息
    case SendHearheat=>{
      //worker节点向master节点汇报心跳
      masterRef ! Hearhear(workerid)
    }
  }
}

object Worker{
  val cores = 4
  val memory = 16
  def main(args: Array[String]): Unit = {
      //master args
    val masterHost = args(0)
    val masterPort = args(1).toInt

    //worker args
    val workerHost = args(2)
    val workerPort = args(3).toInt

    //使用ActorSystem创建Actor，ActorSystem时单例的
    val configStr=
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = localhost
         |akka.remote.netty.tcp.port = 9990
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //创建ActorSystem， 可以创建并管理Actor
    val workerSystem = ActorSystem("WorkerSystem", config)

    //利用masterSystem创建Actor
    workerSystem.actorOf(Props(new Worker(masterHost,masterPort, workerHost, workerPort, cores, memory)), "Worker")

    //给自己发个消息
    //masterRef ! "hello"
  }
}
