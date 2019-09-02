package com.JadePeng.scala.step03.sparkdemo

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.collection.mutable

class Master extends Actor {
  //存储worker节点的注册信息
  val workerIdAndWorkers = new mutable.HashMap[String, WorkerInfo]()
  //最大允许超时时间是10秒中
  val check_interval = 10000

  //用来检测超时的worker节点
  override def preStart(): Unit = {
    val actorRef: ActorRef = self
    import context.dispatcher
    //启动一个定时器，定时向master节点发送心跳信息
    context.system.scheduler.schedule(0 millis, 5000 millis, actorRef, CheckTimeOutWorker)
  }

  /**
    * 接收消息
    *
    * @return
    */
  override def receive: Receive = {
    //worker节点向master节点发送的注册信息（节点id，核数，内存）
    case RegisterWorkInfo(workid, cores, memory) => {
      val workerInfo = new WorkerInfo(workid, cores, memory)
      if (!workerIdAndWorkers.contains(workid)) {
        workerIdAndWorkers.+=((workid, workerInfo))
        //master已经将worker节点的信息保存起来了，然后向worker节点回复一个注册成功的消息
        sender() ! RegisterWorker
      }
    }
    //worker节点发送给master节点的心跳信息，目的是为了报活
    case Hearhear(workerid) => {
      val workerInfo = workerIdAndWorkers(workerid)
      val currentTime: Long = System.currentTimeMillis()
      workerInfo.lastHearheatTime = currentTime
    }

    //master发给自己的消息，用来检测超时的worker节点
    case CheckTimeOutWorker => {
      val workers: Iterable[WorkerInfo] = workerIdAndWorkers.values
      val currentTime: Long = System.currentTimeMillis()
      //返回的是所有超时的worker节点
      val dealWorkers: Iterable[WorkerInfo] = workers.filter(worker => currentTime - worker.lastHearheatTime > check_interval)
      for (dw <- dealWorkers) {
        workerIdAndWorkers -= dw.workerid
      }
      println(s"当前活跃的worker节点数量：${workerIdAndWorkers.values.size}")
    }
  }
}

object Master {
  val MASTER_SYSTEM = "MasterSystem"
  val MASTER_NAME = "Master"

  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    //使用ActorSystem创建Actor，ActorSystem时单例的
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = ${host}
         |akka.remote.netty.tcp.port = ${port}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //创建ActorSystem， 可以创建并管理Actor
    val masterSystem = ActorSystem("MasterSystem", config)

    //利用masterSystem创建Actor
    val masterRef = masterSystem.actorOf(Props[Master], "Master")
    //给自己发个消息
    //masterRef ! "hello"
  }
}
