package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.util.database.QueryDB

/**
  * 数据库的操作类
  *
  * @author Peng
  */
object AnalyzerRuleDB {
  /**
    * 查询过滤规则
    */
  def queryFilterRule() = {
    //sql语句 0: 代表黑名单(需过滤的)  1: 代表白名单
    val sql = "select value from nh_filter_rule where type=0"
    //指定查询的字段
    val field = "value"
    //查询规则信息并返回
    val list = QueryDB.queryData(sql, field)

    list
  }
}
