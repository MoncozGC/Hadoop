package com.air.antispider.stream.dataprocess.businessprocess

import java.text.SimpleDateFormat
import java.util.Date

import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.common.util.spark.SparkMetricsUtils
import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

/**
  * 任务监控类
  *
  * @author Peng
  */
object SparkStreamingMonitor {
  /**
    * (每处理一批次数据就请求一次监控地址获取当前批次计算的结束时间和开始时间得到计算时长)
    * 系统监控
    * 获取数据的处理速度 = 处理时长/数据量
    *
    * @param sc    sparkContext对象
    * @param rdd   rdd.count就是计算的数据量
    * @param jedis 数据需要写入到redis中
    */
  def streamingMonitor(sc: SparkContext, rdd: RDD[String], serversCountMap: collection.Map[String, Int], jedis: JedisCluster) = {
    /**
      * 实现思路：
      * 1：获取applicationId
      * 2：获取applicationName
      * 3：配置监控路径
      * 4：获取json字符串
      * 5：获取gauges节点数据
      * 6：获取任务的开始时间和结束时间，然后取得两个时间的时间间隔得到计算时长
      * 7：获取批次计算数据的总量
      * 8：任务的计算速度
      * 9：将计算出来的结果进行封装到map对象
      * 10：将封装好的数据写入到redis
      */
    //1：获取applicationId
    val applicationId = sc.applicationId
    //2：获取applicationName
    val applicationName = sc.appName
    //3：配置监控路径
    val url = "http://localhost:4040/metrics/json/"
    //4：获取json字符串
    val jsonObject: JSONObject = SparkMetricsUtils.getMetricsJson(url)
    // 5：获取gauges节点数据
    val result: JSONObject = jsonObject.getJSONObject("gauges")
    //6：获取任务的开始时间和结束时间，然后取得两个时间的时间间隔得到计算时长
    //获取开始时间："local-1562291492913.driver.dataprocess-streaming.StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val startTimePath = s"${applicationId}.driver.${applicationName}.StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val startTimeValue: JSONObject = result.getJSONObject(startTimePath)
    var processStartTime: Long = 0
    if (startTimeValue != null) {
      processStartTime = startTimeValue.getLong("value")
    }
    //获取结束时间："local-1562291492913.driver.dataprocess-streaming.StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"
    val endTimePath = s"${applicationId}.driver.${applicationName}.StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"
    val endTimeValue: JSONObject = result.getJSONObject(endTimePath)
    var processEndTime: Long = 0
    if (endTimeValue != null) {
      processEndTime = endTimeValue.getLong("value")
    }
    //7：获取批次计算数据的总量
    val sourceCount = rdd.count()
    //任务计算时间
    val costTime = processEndTime - processStartTime
    //8：任务的计算速度 [计算速度 = 数据量 / 处理时长]
    val countPerMillis: Float = sourceCount.toFloat / costTime.toFloat

    //指定日期转换格式
    //processEndTime: 是一个时间戳 需要转换成时间
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val processingEndTimeString = format.format(new Date(processEndTime))
    //9：将计算出来的结果进行封装到map对象
    val fieldMap = scala.collection.mutable.Map(
      "endTime" -> processingEndTimeString,
      "applicationUniqueName" -> applicationName.toString,
      "applicationId" -> applicationId.toString,
      "sourceCount" -> sourceCount.toString,
      "costTime" -> costTime.toString,
      "countPerMillis" -> countPerMillis.toString,
      "serversCountMap" -> serversCountMap)

    //10：将封装好的数据写入到redis
    val keyName = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties") + System.currentTimeMillis().toString
    //val keyNameLast = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties")+ "_LAST"
    val expTime = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt

    //将数据写入redis
    jedis.setex(keyName, expTime, Json(DefaultFormats).write(fieldMap))
    //jedis.setex(keyNameLast, expTime, Json(DefaultFormats).write(fieldMap))
  }

}
