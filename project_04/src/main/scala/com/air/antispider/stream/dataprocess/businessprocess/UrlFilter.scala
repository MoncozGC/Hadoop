package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.AccessLog

import scala.collection.mutable.ArrayBuffer

/**
  * 数据清洗
  */
object UrlFilter {
  //过滤掉符合过滤规则的数据
  def filterUrl(accessLog: AccessLog, filterRuleList: ArrayBuffer[String]): Boolean = {
//    println(accessLog+"======accessLog======")
//    println(filterRuleList+"=======filterRuleList==========")
    /**
      * 实现思路：
      * 1：设置一个变量flag=true
      * 2：循环遍历过滤规则，根据请求的地址正则匹配，匹配上则flag=false
      * 3：将flag返回
      */
    var isMatch = true
    filterRuleList.foreach(rule=>{
      if(accessLog.request.matches(rule)){
        isMatch = false
      }
    })
    isMatch
  }

}
