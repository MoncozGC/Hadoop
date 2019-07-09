package com.air.antispider.stream.rulecompute.businessprocess

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.air.antispider.stream.common.bean.{FlowCollocation, RuleCollocation}
import com.air.antispider.stream.common.util.database.{QueryDB, c3p0Util}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 实时计算数据, 数据库操作类
  *
  * @author Peng
  */
object AnalyzerRuleDB {


  /**
    * 读取关键页面数据
    */
  def queryCriticalPageList(): ArrayBuffer[String] = {
    //指定sql语句
    val sql: String = "select criticalPageMatchExpression from nh_query_critical_pages"
    //指定查询字段
    val field: String = "criticalPageMatchExpression"

    //执行查询操作
    val result: ArrayBuffer[String] = QueryDB.queryData(sql, field)
    result
  }

  def queryIpBlackList(): ArrayBuffer[String] = {
    //指定sql语句
    val sql = "select ip_name from nh_ip_blacklist"
    //指定查询字段
    val field = "ip_name"
    //执行查询操作
    val result = QueryDB.queryData(sql, field)
    result
  }

  /**
    * 读取流程规则数据
    */
  def queryFlow() = {
    //返回的是流程的列表, 有可能有多个流程(配错了), 有可能一个流程没有(配错了)
    val arrFlows = new ArrayBuffer[FlowCollocation]()
    //查询流程列表  流程id(info.id) 流程的名字(info.process_name) 阈值(strategy.crawler_blacklist_thresholds)
    val sql =
      """
        |select info.id,info.process_name,strategy.crawler_blacklist_thresholds
        |	from nh_process_info info,	nh_strategy strategy
        |	where info.id=strategy.id and status=0
      """.stripMargin

    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        //流程ID
        val flowId = rs.getString("id")
        //流程名字
        val flowName = rs.getString("process_name")
        //策略阈值(当用户打分超过这个值就是爬虫ip)
        val flowLimitScore = rs.getDouble("crawler_blacklist_thresholds")

        // createRuleList 流程的规则, 需要进行表关联
        arrFlows += FlowCollocation(flowId, flowName, createRuleList(flowId, 0), flowLimitScore, flowId)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      c3p0Util.close(conn, ps, rs)
    }
    arrFlows
  }

  /**
    * 获取规则列表
    *
    * @param process_id 根据该ID查询规则
    * @param n          0:表示反爬虫  1: 表示防占座
    * @return list列表
    */
  def createRuleList(process_id: String, n: Int): List[RuleCollocation] = {
    var list = new ListBuffer[RuleCollocation]


    /*    val sql =
          """
            |select * FROM
            |	(select nh_rule.id,nh_rule.process_id,nh_rules_maintenance_table.rule_real_name,nh_rule.rule_type,nh_rule.crawler_type,nh_rule.status,nh_rule.arg0,nh_rule.arg1,nh_rule.score
            |	from nh_rule,nh_rules_maintenance_table
            |	where nh_rules_maintenance_table.rule_name=nh_rule.rule_name)
            |	as tab where process_id = '" + process_id + "'and crawler_type="+n
          """.stripMargin*/
    val sql = "select * from(select nh_rule.id,nh_rule.process_id,nh_rules_maintenance_table.rule_real_name,nh_rule.rule_type,nh_rule.crawler_type," +
      "nh_rule.status,nh_rule.arg0,nh_rule.arg1,nh_rule.score from nh_rule,nh_rules_maintenance_table where nh_rules_maintenance_table." +
      "rule_name=nh_rule.rule_name) as tab where process_id = '" + process_id + "'and crawler_type=" + n
    //and status="+n
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        val ruleId = rs.getString("id")
        val flowId = rs.getString("process_id")
        val ruleName = rs.getString("rule_real_name")
        val ruleType = rs.getString("rule_type")
        val ruleStatus = rs.getInt("status")
        val ruleCrawlerType = rs.getInt("crawler_type")
        val ruleValue0 = rs.getDouble("arg0")
        val ruleValue1 = rs.getDouble("arg1")
        val ruleScore = rs.getInt("score")
        val ruleCollocation = new RuleCollocation(ruleId, flowId, ruleName, ruleType, ruleStatus, ruleCrawlerType, ruleValue0, ruleValue1, ruleScore)
        list += ruleCollocation
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      c3p0Util.close(conn, ps, rs)
    }
    list.toList
  }
}
