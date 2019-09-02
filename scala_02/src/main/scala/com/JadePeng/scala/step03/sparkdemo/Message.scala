package com.JadePeng.scala.step03.sparkdemo

/**
  * Worker向master注册信息
  */
case class RegisterWorkInfo(workid:String, cores:Int, memory:Int)

/**
  * worker节点的资源信息
  * @param workerid
  * @param cores
  * @param memory
  */
class WorkerInfo(val workerid:String, val cores:Int, val memory:Int){
  //最后报活时间
  var lastHearheatTime:Long  = _
}

/**
  * master向worker反馈注册成功的消息
  */
case object RegisterWorker

/**
  * 向worker本身发送消息
  */
case object SendHearheat

/**
  * worker节点向master节点发送心跳信息
  * @param workid
  */
case class Hearhear(val workid:String)

/**
  * 定时检测超时的worker节点
  */
case object CheckTimeOutWorker