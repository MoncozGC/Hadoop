package com.air.antispider.stream.dataprocess.businessprocess

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.air.antispider.stream.common.bean.AnalyzeRule
import com.air.antispider.stream.common.util.database.{QueryDB, c3p0Util}
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 预处理数据, 数据库的操作类
  */
object AnalyzerRuleDB {

  /**
    * 查询过滤规则
    *
    * @return
    */
  def queryFilterRule(): ArrayBuffer[String] = {
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
  def queryClassifyRule(): mutable.HashMap[String, ArrayBuffer[String]] = {
    //国内查询
    val nqSql = "select expression from nh_classify_rule where flight_type=" + FlightTypeEnum.National.id + " and operation_type=" + BehaviorTypeEnum.Query.id

    //国际查询
    val iqSql = "select expression from nh_classify_rule where flight_type=" + FlightTypeEnum.International.id + " and operation_type=" + BehaviorTypeEnum.Query.id

    //国内预定
    val nbSql = "select expression from nh_classify_rule where flight_type=" + FlightTypeEnum.National.id + " and operation_type=" + BehaviorTypeEnum.Book.id

    //国际预定
    val ibSql = "select expression from nh_classify_rule where flight_type=" + FlightTypeEnum.International.id + " and operation_type=" + BehaviorTypeEnum.Book.id

    //指定查询的字段
    val field = "expression"

    //查询操作
    val nqList: ArrayBuffer[String] = QueryDB.queryData(nqSql, field)
    val iqList = QueryDB.queryData(iqSql, field)
    val nbList = QueryDB.queryData(nbSql, field)
    val ibList = QueryDB.queryData(ibSql, field)

    //创建Map， 将规则数据进行封装到map对象  这样只需要广播一次
    val ruleMap: mutable.HashMap[String, ArrayBuffer[String]] = new mutable.HashMap[String, ArrayBuffer[String]]()
    ruleMap.put("NationalQueryList", nqList)
    ruleMap.put("InternationalQueryList", iqList)
    ruleMap.put("NationalBookList", nbList)
    ruleMap.put("InternationalBookList", ibList)
    //返回HashMap
    ruleMap
  }

  /**
    * 读取数据库中的请求解析规则
    */
  def queryRule(behaviorType: Int): List[AnalyzeRule] = {
    val analyzeRuleList = new ArrayBuffer[AnalyzeRule]()
    //定义查询的sql
    val sql = "select * from analyzerule where behavior_type=" + behaviorType

    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        val analyzeRule = new AnalyzeRule()
        analyzeRule.id = rs.getString("id")
        analyzeRule.flightType = rs.getString("flight_type").toInt
        analyzeRule.BehaviorType = rs.getString("behavior_type").toInt
        analyzeRule.requestMatchExpression = rs.getString("requestMatchExpression")
        analyzeRule.requestMethod = rs.getString("requestMethod")
        analyzeRule.isNormalGet = rs.getString("isNormalGet").toBoolean
        analyzeRule.isNormalForm = rs.getString("isNormalForm").toBoolean
        analyzeRule.isApplicationJson = rs.getString("isApplicationJson").toBoolean
        analyzeRule.isTextXml = rs.getString("isTextXml").toBoolean
        analyzeRule.isJson = rs.getString("isJson").toBoolean
        analyzeRule.isXML = rs.getString("isXML").toBoolean
        analyzeRule.formDataField = rs.getString("formDataField")
        analyzeRule.book_bookUserId = rs.getString("book_bookUserId")
        analyzeRule.book_bookUnUserId = rs.getString("book_bookUnUserId")
        analyzeRule.book_psgName = rs.getString("book_psgName")
        analyzeRule.book_psgType = rs.getString("book_psgType")
        analyzeRule.book_idType = rs.getString("book_idType")
        analyzeRule.book_idCard = rs.getString("book_idCard")
        analyzeRule.book_contractName = rs.getString("book_contractName")
        analyzeRule.book_contractPhone = rs.getString("book_contractPhone")
        analyzeRule.book_depCity = rs.getString("book_depCity")
        analyzeRule.book_arrCity = rs.getString("book_arrCity")
        analyzeRule.book_flightDate = rs.getString("book_flightDate")
        analyzeRule.book_cabin = rs.getString("book_cabin")
        analyzeRule.book_flightNo = rs.getString("book_flightNo")
        analyzeRule.query_depCity = rs.getString("query_depCity")
        analyzeRule.query_arrCity = rs.getString("query_arrCity")
        analyzeRule.query_flightDate = rs.getString("query_flightDate")
        analyzeRule.query_adultNum = rs.getString("query_adultNum")
        analyzeRule.query_childNum = rs.getString("query_childNum")
        analyzeRule.query_infantNum = rs.getString("query_infantNum")
        analyzeRule.query_country = rs.getString("query_country")
        analyzeRule.query_travelType = rs.getString("query_travelType")
        analyzeRule.book_psgFirName = rs.getString("book_psgFirName")
        analyzeRuleList += analyzeRule
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      c3p0Util.close(conn, ps, rs)
    }
    analyzeRuleList.toList
  }

  /**
    * 读取黑名单ip信息
    */
  def queryIpBlackList(): ArrayBuffer[String] = {
    //编写sql
    val sql = "select ip_name from nh_ip_blacklist"
    //查询字段
    val field = "ip_name"

    val list = QueryDB.queryData(sql, field)

    list
  }

}
