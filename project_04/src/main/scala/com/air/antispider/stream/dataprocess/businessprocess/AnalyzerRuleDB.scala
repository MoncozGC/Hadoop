package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.util.database.QueryDB

import scala.collection.mutable.ArrayBuffer

/**
  * 数据库的操作类
  */
object AnalyzerRuleDB {

  /**
    * 查询过滤规则
    * @return
    */
  def queryFilterRule() = {
    //编写sql语句
    val sql = "select value from nh_filter_rule where type=0"

    //指定要查询的字段
    val field = "value"

    //查询规则信息并返回
    val list: ArrayBuffer[String] =  QueryDB.queryData(sql, field)
    list
  }
}
