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
    * 2.1 计算单位时间内ip段的访问量
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
    * 2.2 某个ip, 单位时间内ip的访问量
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
    * 2.3 计算单位时间内关键页面的访问量
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
    * 2.4 某个ip，单位时间内ua的访问种类数
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
    * 2.5 某个ip, 单位时间内关键页面的最短访问间隔
    *
    * @param queryDataPackageDStream
    * @param windowDuration
    * @param sideDuration
    * @param queryCriticalPages
    */
  def criticalPageAccTime(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration, sideDuration: Duration, queryCriticalPages: ArrayBuffer[String]): DStream[(String, Iterable[String])] = {
    /**
      * 代码思路：
      * 1. 拿到kafka的数据
      * 2. 从kafka中拿到request
      * 3. 匹配广播变量的数据(remoteAdd，timeIso8601)
      * 4. 拿到上面的数据，循环出来每个remoteAddr和他的时间
      * 5. 对timeIso8601进行排序，然后取到相邻的访问时间的间隔
      * 6. 将remoteAad和timeIso8601进行排序，然后取到最小的访问间隔
      */
    queryDataPackageDStream.map(processData => {
      //拿到request
      val request = processData.request
      var flag = false
      //循环匹配广播变量中的关键页面正则表达式
      queryCriticalPages.foreach(page => {
        if (request.matches(page)) {
          flag = true
        }
      })
      //如果匹配上
      if (flag) {
        (processData.remoteAddr, processData.timeIso8601)
      } else {
        ("", "NULL")
      }
    }).groupByKeyAndWindow(windowDuration, sideDuration)

  }


  /**
    * 2.6 某个ip，单位时间内小于最短访问时间（自设）的关键页面的查询次数
    *
    * @param queryDataPackageDStream
    * @param windowDuration
    * @param sideDuration
    * @param queryCriticalPages
    * @return
    */
  def aCriticalPageAccTime(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration, sideDuration: Duration, queryCriticalPages: ArrayBuffer[String]) = {
    /**
      * 实现思路：
      * 1：拿到kafka的数据以及关键页面的广播变量
      * 2：从kafka中获取request数据，匹配关键页面
      * 3：匹配成功：返回（（remoteAddr, request）， timeIso8601），并进行窗口函数操作
      * 4：拿到上面的数据进行map操作，循环出来每个（remoteAddr， request）和timeIso8601
      * 5： 对timeIso8601进行循环操作，计算临近的两个时间差
      * 6：对上面的arrayBuffer进行迭代操作，大于预设值就累加count
      * 7：将数据收集起来
      */
    queryDataPackageDStream.map(processData => {
      //获取request请求
      val request: String = processData.request
      var flag = false
      //匹配关键页面
      queryCriticalPages.foreach(page => {
        if (request.matches(page)) {
          flag = true
        }
      })
      //匹配上
      if (flag) {
        ((processData.remoteAddr, request), processData.timeIso8601)
      } else {
        (("", ""), "NULL")
      }
    }).groupByKeyAndWindow(windowDuration, sideDuration)
  }

  /**
    * 2.7 某个ip，单位时间内查询不同行程的次数
    *
    * @param queryDataPackageDStream
    * @param windowDuration
    * @param sideDuration
    * @return
    */
  def flightQuery(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration, sideDuration: Duration): DStream[(String, Iterable[(String, String)])] = {
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

  /**
    * 2.8 某个ip, 单位时间内关键页面的访问次数的cookie数
    *
    * @param queryDataPackageDStream
    * @param windowDuration
    * @param sideDuration
    * @param queryCriticalPages
    */
  def cookieCounts(queryDataPackageDStream: DStream[ProcessedData], windowDuration: Duration, sideDuration: Duration, queryCriticalPages: ArrayBuffer[String]): DStream[(String, Iterable[String])] = {
    /**
      * 1、	拿到kafka数据，关键页面的广播变量，窗口时间，滑动时间进行计算
      * 2、	从kafka数据中取到request，匹配关键页面
      * 3、	匹配成功，记录（remoteAddr，JSESSIONID），并做groupByKey 操作，将相同remoteAddr的JSESSIONID统计到ArrayBuffer中
      * 4、	拿到上面的数据进行去重count，最终封装数据（remoteAddr，JSESSIONIDCount）
      * 5、	最后将（remoteAddr，JSESSIONIDCount最小值）收集到map中
      */
    queryDataPackageDStream.map(processData => {
      //拿到request
      val request = processData.request
      var flag = false
      //循环匹配广播变量中的关键页面正则表达式
      queryCriticalPages.foreach(page => {
        if (request.matches(page)) {
          flag = true
        }
      })

      //判断是否关键页面
      if (flag) {
        (processData.remoteAddr, processData.cookieValue_JSESSIONID)
      } else {
        ("", "NULL")
      }
    }).groupByKeyAndWindow(windowDuration, sideDuration)
  }

}
