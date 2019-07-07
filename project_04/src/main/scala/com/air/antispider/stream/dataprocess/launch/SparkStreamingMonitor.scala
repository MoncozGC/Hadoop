package com.air.antispider.stream.dataprocess.launch

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
      * 实现思路:
      * 1. 获取applicationId
      * 2. 获取appName
      * 3. 配置监控路径
      * 4. 获取json字符串
      * 5. 获取gauges节点数据
      * 6. 获取批次的结束时间和开始时间 ==> 批次的计算时间
      * 7. 获取批处理的数据量(rdd.count)
      * 8. 计算任务的计算速度 [数据的处理速度 = 处理时长/数据量]
      * 9. 将计算处理的数据封装到Map集合中
      * 10. 将封装好的数据发送到redis中
      */
    //1. 获取applicationId
    val applicationId: String = sc.applicationId
    //2. 获取appName
    val applicationName: String = sc.appName
    //3. 配置监控路径
    val url: String = "http://localhost:4040/metrics/json/"
    //4. 获取json字符串
    val jsonObject: JSONObject = SparkMetricsUtils.getMetricsJson(url)
    //5. 获取gauges节点数据  getJSONObject: 根据key获取value
    val result: JSONObject = jsonObject.getJSONObject("gauges")
    //6. 获取批次的结束时间和开始时间 ==> 批次的计算时间
    //开始时间: "local-1562489597606.driver.dataprocess-streaming.StreamingMetrics.streaming.lastReceivedBatch_processingStartTime": {"value": 1562489705020 }
    //获取大key "local....EndTime"
    val startTimePath: String = s"${applicationId}.driver.s${applicationName}.StreamingMetrics.streaming.lastReceivedBatch_processingStartTime"
    //获取value  {"value":....}
    val startTimeValue: JSONObject = result.getJSONObject(startTimePath)
    var processStartTime: Long = 0
    if (startTimeValue != null) {
      processStartTime = startTimeValue.getLong("value")
    }
    //结束时间 "local-1562489597606.driver.dataprocess-streaming.StreamingMetrics.streaming.lastReceivedBatch_processingEndTime": {"value": 1562489706228 }
    val endTimePath: String = s"${applicationId}.driver.${applicationName}.StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"
    val endTimeValue: JSONObject = result.getJSONObject(endTimePath)
    var processEndTime: Long = 0
    if (endTimeValue != null) {
      processEndTime = endTimeValue.getLong("value")
    }
    //7. 获取批处理的数据量(rdd.count)
    val sourceCount: Long = rdd.count()
    //8. 计算任务的计算速度 [计算速度 = 数据量 / 处理时长]
    //任务计算时间
    val costTime: Long = processEndTime - processStartTime
    //计算速度
    var countPerMillis: Float = costTime.toFloat / sourceCount.toFloat

    //9. 将计算出来的结果进行封装到map对象
    val format = new SimpleDateFormat("yyyy-MM-dd HH:dd:ss")
    //processEndTime: 是一个时间戳 需要转换成时间
    val processingEndTimeString = format.format(new Date(processEndTime))
    val fieldMap = scala.collection.mutable.Map(
      "endTime" -> processingEndTimeString,
      "applicationUniqueName" -> applicationName.toString,
      "applicationId" -> applicationId.toString,
      "sourceCount" -> sourceCount.toString,
      "costTime" -> costTime.toString,
      "countPerMillis" -> countPerMillis.toString,
      "serversCountMap" -> serversCountMap)

    //10. 将封装好的数据发送到redis中
    //实时数据
    val keyName = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties")
    //最新的数据
    //val keyNameLast = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties")+ "_LAST"
    val expTime = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt

    //将数据写入到redis中
    jedis.setex(keyName, expTime, Json(DefaultFormats).write(fieldMap))
  }

}
