package com.air.antispider.stream.rulecompute.businessprocess


import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.air.antispider.stream.common.bean.{FlowCollocation, RuleCollocation}
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer


/**
  * 规则处理公共类
  *
  * @author Peng
  */
object RuleUtil {


  /**
    * 返回所有的时间列表
    * 功能：将时间类型的字符串转换成十进制的时间戳
    *
    * @param accTimes 访问时间
    * @return
    */
  def allTimeList(accTimes: Iterable[String]): util.ArrayList[Long] = {
    //定义一个集合放时间间隔
    val timeList: util.ArrayList[Long] = new util.ArrayList[Long]()
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    accTimes.foreach(time => {
      //访问时间不为空
      if (!"NULL".equalsIgnoreCase(time)) {
        //将字符串转换成日期
        val timeStr: Date = new DateTime(time).toDate
        //转换成定义的日期
        val dt = sdf.format(timeStr)
        //将时间转换成一个十进制的时间戳
        val tmpTime = sdf.parse(dt).getTime
        //添加到集合中
        timeList.add(tmpTime)
      }
    })
    //返回
    timeList
  }


  /**
    * 获取访问时间列表中的最小的时间间隔
    *
    * @param accTimesList 统计出来所有的时间
    * @return 最小的时间间隔
    */
  def minInterval(accTimesList: util.ArrayList[Long]): Int = {
    //计算时间间隔
    val intervalList: util.ArrayList[Long] = allInterval(accTimesList)

    //排序
    val result: Array[AnyRef] = intervalList.toArray()
    util.Arrays.sort(result)
    //返回第一个, 最小的时间间隔
    result(0).toString.toInt
  }

  /**
    * 获取所有的访问间隔
    *
    * @param accTimeList 统计出来所有的时间
    * @return 所有的访问间隔
    */
  def allInterval(accTimeList: util.ArrayList[Long]): util.ArrayList[Long] = {
    //排序
    val arr = accTimeList.toArray()
    util.Arrays.sort(arr)
    //创建list用来封装时间间隔
    val intervalList = new util.ArrayList[Long]()
    //计算两次访问的时间间隔
    if (arr.length > 1) {
      // until: 返回传过去的值但是不包含最后一个
      for (i <- 1 until (arr.length - 1)) {
        //返回最后一个
        val time1 = arr(i - 1).toString.toLong
        //返回当前
        val time2 = arr(i).toString.toLong
        val interval = time2 - time1
        intervalList.add(interval)
      }
    }
    //返回所有的访问间隔
    intervalList
  }

  /**
    * 计算预设值的关键页面的查询次数
    * 条件: 5分钟内小于最短访问间隔___的关键页面查询次数 < __
    * 时间小于就累加, 不小于就不累加, 然后获取它的查询次数
    *
    * @param accTimes 访问时间
    * @param flowList 广播流程规则
    * @return
    */
  def calcInterval(accTimes: Iterable[String], flowList: ArrayBuffer[FlowCollocation]): Int = {
    //查找流程，因为广播变量中可能会有多个流程
    val filterRuleList: List[RuleCollocation] = flowList(0).rules
    //查找下标是0的流程的规则列表, 获取第一个参数的值
    // 5分钟内小于最短访问间隔___的关键页面查询次数 < __
    // xx_rule表 arg0是第一个参数  arg1是第二个参数
    val defaultMinInterval: Double = filterRuleList.filter(rule => rule.ruleName == ("criticalPagesLessThanDefault"))(0).ruleValue0
    //预设count
    var accCount: Int = 0
    //统计时间，将传递的日志时间转换成日期时间戳, 时间戳才好找时间间隔, 日期字符串不好使用
    val allTime: util.ArrayList[Long] = allTimeList(accTimes)
    //取出来所有的时间间隔
    val intervalList: util.ArrayList[Long] = allInterval(allTime)

    //intervalList不为空且大于0
    if (intervalList != null && intervalList.size() > 0) {
      //遍历获取, 当不取最后一个
      for (i <- 1 until intervalList.size()) {
        //如果时间间隔小于预设的值, 就进行累加
        if (intervalList.get(i) > defaultMinInterval) {
          accCount = accCount + 1
        }
      }
    }
    //查询次数
    accCount
  }
}
