package com.air.antispider.stream.rulecompute.businessprocess

import com.air.antispider.stream.common.bean._
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum, TravelTypeEnum}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.streaming.dstream.DStream

/**
  * kafka中的字符串转换成bean对象
  *
  * @author Peng
  */
object QueryDataPackage {
  /**
    * kafka中的字符串转换成bean对象
    *
    * @param message 接收的kafka字符串数据
    * @return 封装对象返回 DStream[ProcessedData]
    */
  def queryDataLoadAndPackage(message: DStream[String]): DStream[ProcessedData] = {
    message.mapPartitions(partitions => {
      //一个分区实例化一个对象
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      //字符串转换bean对象
      partitions.map(record => {
        //分割数据
        val dataArray = record.split("#CS#")
        //sourceData 原始数据，并没有传递，只是一个占位
        val sourceData = dataArray(0)
        //requestMethod 请求方法
        val requestMethod = dataArray(1)
        //request 请求路径
        val request = dataArray(2)
        //remoteAddr 客户端ip
        val remoteAddr = dataArray(3)
        //httpUserAgent 代理
        val httpUserAgent = dataArray(4)
        //timeIso8601 时间
        val timeIso8601 = dataArray(5)
        //serverAddr 请求的服务器地址
        val serverAddr = dataArray(6)
        //highFrqIPGroup 高频ip 如果是true就返回true, false就返回false  忽略大小写
        val highFrqIPGroup = dataArray(7).equalsIgnoreCase("true")
        //请求类型
        val requestType = RequestType(FlightTypeEnum.withName(dataArray(8)), BehaviorTypeEnum.withName(dataArray(9)))
        //往返类型
        val travelType = TravelTypeEnum.withName(dataArray(10))
        //requestParams 核心请求参数，飞行时间、目的地、出发地
        val requestParams = CoreRequestParams(dataArray(11), dataArray(12), dataArray(13))
        //cookie中的jessonId
        val cookieValue_JSESSIONID = dataArray(14)
        //cookie中的userId
        val cookieValue_USERID = dataArray(15)

        //判断是否为空
        val queryRequestData = if (!dataArray(16).equalsIgnoreCase("NULL")) {
          //readValue: 读取value
          mapper.readValue(dataArray(16), classOf[QueryRequestData]) match {
            //数据不为空直接返回
            case value if value != null => Some(value)
            case _ => None
          }
        } else {
          None
        }
        //消费的是查询的topic信息, 预定不会有信息(ComputeLaunch中指定的topic) 17
        val bookRequestData: Option[BookRequestData] = None
        val httpReferrer = dataArray(18)

        //封装对象返回
        ProcessedData(sourceData, requestMethod, request, remoteAddr, httpUserAgent, timeIso8601,
          serverAddr, highFrqIPGroup, requestType, travelType, requestParams, cookieValue_JSESSIONID, cookieValue_USERID,
          queryRequestData, bookRequestData, httpReferrer)
      })
    })

  }
}
