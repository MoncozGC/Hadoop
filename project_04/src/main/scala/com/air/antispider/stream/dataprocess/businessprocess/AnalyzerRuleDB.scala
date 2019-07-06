package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.util.database.QueryDB
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 数据库的操作类
  */
object AnalyzerRuleDB {

  /**
    * 查询过滤规则
    *
    * @return
    */
  def queryFilterRule() = {
    //编写sql语句
    val sql = "select value from nh_filter_rule where type=0"

    //指定要查询的字段
    val field = "value"

    //查询规则信息并返回
    val list: ArrayBuffer[String] = QueryDB.queryData(sql, field)
    list
  }

  /**
    * 查询分类规则信息
    */
  def queryClassifyRule() = {
    //国内查询
    val nqSql = "select expression from nh_classify_rule where flight_type="+FlightTypeEnum.National.id+" and operation_type="+BehaviorTypeEnum.Query.id

    //国际查询
    val iqSql = "select expression from nh_classify_rule where flight_type="+FlightTypeEnum.International.id+" and operation_type="+BehaviorTypeEnum.Query.id

    //国内预定
    val nbSql = "select expression from nh_classify_rule where flight_type="+FlightTypeEnum.National.id+" and operation_type="+BehaviorTypeEnum.Book.id

    //国际预定
    val ibSql = "select expression from nh_classify_rule where flight_type="+FlightTypeEnum.International.id+" and operation_type="+BehaviorTypeEnum.Book.id

    //指定查询的字段
    val field = "expression"

    //查询操作
    val nqList: ArrayBuffer[String] = QueryDB.queryData(nqSql, field)
    val iqList = QueryDB.queryData(iqSql, field)
    val nbList = QueryDB.queryData(nbSql, field)
    val ibList = QueryDB.queryData(ibSql, field)

    //创建Map， 将规则数据进行封装到map对象  这样只需要广播一次
    val ruleMap = new mutable.HashMap[String, ArrayBuffer[String]]()
    ruleMap.put("NationalQueryList", nqList)
    ruleMap.put("InternationalQueryList", iqList)
    ruleMap.put("NationalBookList", nbList)
    ruleMap.put("InternationalBookList", ibList)
    ruleMap
  }
}
