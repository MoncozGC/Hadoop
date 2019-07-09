package com.air.antispider.stream.rulecompute.businessprocess

import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

/**
  * 指标核心计算类
  *
  * @author Peng
  */
object CoreRule {


  /**
    * 计算单位时间内ip段的访问量
    *
    * @param queryDataPackageDStream kafka Bean对象的数据
    * @param windowDuration          窗口长度
    * @param sideDuration            滑动距离
    * @return DStream[(ip段, 访问的次数(count))]
    */
  def ipBlockAccessCount(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration, sideDuration: Duration): DStream[(String, Int)] = {
    /**
      * 实现思路:
      * 1. 拿到kafka中的数据, 设置窗口长度 滑动距离
      * 2. 判断remoteIP地址是否有效, 如果有数据, 就进行ip的拆分
      * 3. 通过窗口函数进行计算指标(reduceByKeyAndWindow)
      */
    queryDataPackageDStream.map(processData => {
      //判断ip是否有效  数据只有可能是一个ip地址或者NULL
      if (processData.remoteAddr.equalsIgnoreCase("NULL")) {
        (processData.remoteAddr, 0)
      } else {
        //有数据 indexOf返回该字符串中第一次出现的索引 两个参数: 从指定字符串开始
        val index = processData.remoteAddr.indexOf(".")
        val ipBlock = processData.remoteAddr.substring(0, processData.remoteAddr.indexOf(".", index + 1))
        //设置返回值  来一条数据加一个1
        (ipBlock, 1)
      }
    }).reduceByKeyAndWindow((x: Int, y: Int) => (x + y), windowDuration, sideDuration)

  }

  /**
    * 某个ip, 单位时间内ip的访问量
    * 线上5分钟, 测试10秒钟
    *
    * @param queryDataPackageDStream
    * @param windowDuration
    * @param sideDuration
    */
  def ipAccessCount(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration, sideDuration: Duration): DStream[(String, Int)] = {
    /**
      * 实现思路:
      * 1. 拿到kafka中的数据, 窗口长度 滑动距离
      * 2. 判断remoteIP地址是否有效, 如果有数据就进行累加
      * 3. 通过窗口函数进行计算指标(reduceByKeyAndWindow)
      */
    queryDataPackageDStream.map(processData => {
      //判断remoteIP地址是否有效
      if (processData.remoteAddr.equalsIgnoreCase("NULL")) {
        (processData.remoteAddr, 0)
      } else {
        (processData.remoteAddr, 1)
      }
    }).reduceByKeyAndWindow((x: Int, y: Int) => (x + y), windowDuration, sideDuration)
  }

  /**
    * 计算单位时间内关键页面的访问量
    * 使用窗口函数，线上5分钟，测试10秒钟
    *
    * @param queryDataPackageDStream
    * @param windowDuration
    * @param sideDuration
    * @param queryCriticalPages 关键页面
    */
  def criticalPageCount(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration,
                        sideDuration: Duration, queryCriticalPages: ArrayBuffer[String]) = {
    /**
      * 实现思路:
      * 1. 拿到kafka中的数据和广播变量中的关键页面
      * 2. 从kafka中获取request, 匹配关键页面
      * 3. 匹配成功（remoteAdd, 1）
      * 4. 匹配失败（remoteAdd， 0）
      * 5. 统计最终结果
      */
    queryDataPackageDStream.map(processData => {
      val request = processData.request
      var flag = false
      //循环匹配广播变量中的关键页面正则表达式
      queryCriticalPages.foreach(page => {
        //进行正则匹配
        if (request.matches(page)) {
          flag = true
        }
      })
      //如果匹配上
      if (flag) {
        (processData.remoteAddr, 1)
      } else {
        (processData.remoteAddr, 0)
      }
    }).reduceByKeyAndWindow((x: Int, y: Int) => (x + y), windowDuration, sideDuration)
  }

  /**
    * 某个ip，单位时间内ua的访问种类数
    *
    * @param queryDataPackageDStream
    * @param windowDuration
    * @param sideDuration
    */
  def userAgentAccessCount(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration, sideDuration: Duration): DStream[(String, Iterable[String])] = {
    /**
      * 实现思路:
      * 1. 获取kafka中的数据
      * 2. 获取remoteAdd作为key User-Agent作为value
      * 3. 统计useragent的种类数
      */
    queryDataPackageDStream.map(processData => {
      (processData.remoteAddr, processData.httpUserAgent)
    }).groupByKeyAndWindow(windowDuration, sideDuration)

  }

  /**
    * 某个ip，单位时间内查询不同行程的次数
    *
    * @param queryDataPackageDStream
    * @param windowDuration
    * @param sideDuration
    * @return
    */
  def flightQuery(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration, sideDuration: Duration) = {
    /**
      * 实现思路:
      * 1. 获取到kafka中的数据
      * 2. 使用remoteAdd作为key, (始发地, 目的地)作为value --> 数据在核心参数中requestParams
      * 3. 将数据返回
      */
    queryDataPackageDStream.map(processData => {
      //服务器ip  (始发地, 目的地)
      (processData.remoteAddr, (processData.requestParams.arrcity, processData.requestParams.depcity))
    }).groupByKeyAndWindow(windowDuration, sideDuration)
  }
}
