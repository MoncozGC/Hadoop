package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.AnalyzeRule
import com.air.antispider.stream.dataprocess.constants.BehaviorTypeEnum
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 广播变量监视类
  */
object BroadcastProcess {


  /**
    * 监视过滤规则是否发生了改变
    *
    * @param sc
    * @param filterRuleRef
    * @param jedis
    * @return
    */
  def monitorFilterRule(sc: SparkContext, filterRuleRef: Broadcast[ArrayBuffer[String]], jedis: JedisCluster) = {
    /**
      * 思路实现：
      * 1：获取redis里面的标记
      * 2：判断标记是否发生了改变
      *   2.1：重新获取数据库的过滤规则信息
      *   2.2：将当前广播变量的值进行删除
      *   2.3：将redis的标记重置
      *   2.4：将数据重新的广播
      * 3：如果标记没有发生改变
      * 将filterRuleRef直接返回
      */
    //1：获取redis里面的标记
    val needUpdateFilterRuleList = jedis.get("FilterRuleChangeFlag")

    //2：判断标记是否发生了改变
    //needUpdateFilterRuleList!=null: 判断redis的key是否存在
    //!needUpdateFilterRuleList.isEmpty : 判断redis的value值不是空
    if (needUpdateFilterRuleList != null && !needUpdateFilterRuleList.isEmpty && needUpdateFilterRuleList.toBoolean) {
      //2.1：重新获取数据库的过滤规则信息
      val filterRuleListUpdate = AnalyzerRuleDB.queryFilterRule()
      //2.2：将当前广播变量的值进行删除
      filterRuleRef.unpersist()
      //2.3：将redis的标记重置
      jedis.set("FilterRuleChangeFlag", "false")
      //2.4：将数据重新的广播
      sc.broadcast(filterRuleListUpdate)
    } else {
      filterRuleRef
    }
  }

  /**
    * 监视数据分类规则是否发生了改变
    *
    * @param sc
    * @param ruleMapRef
    * @param jedis
    * @return
    */
  def monitorClassifyRule(sc: SparkContext, ruleMapRef: Broadcast[mutable.HashMap[String, ArrayBuffer[String]]], jedis: JedisCluster): Broadcast[mutable.HashMap[String, ArrayBuffer[String]]] = {
    /**
      * 思路实现：
      * 1：获取redis里面的标记
      * 2：判断标记是否发生了改变
      *   2.1：重新获取数据库的过滤规则信息
      *   2.2：将当前广播变量的值进行删除
      *   2.3：将redis的标记重置
      *   2.4：将数据重新的广播
      * 3：如果标记没有发生改变
      * 将ruleMapRef直接返回
      */
    //1：获取redis里面的标记
    val needUpdateClassifyRuleList = jedis.get("ClassifyRuleChangeFlag")

    //2：判断标记是否发生了改变
    //needUpdateClassifyRuleList!=null: 判断redis的key是否存在
    //!needUpdateClassifyRuleList.isEmpty : 判断redis的value值不是空
    if (needUpdateClassifyRuleList != null && !needUpdateClassifyRuleList.isEmpty && needUpdateClassifyRuleList.toBoolean) {
      //2.1：重新获取数据库的过滤规则信息
      val classifyRuleListUpdate = AnalyzerRuleDB.queryClassifyRule()
      //2.2：将当前广播变量的值进行删除
      ruleMapRef.unpersist()
      //2.3：将redis的标记重置
      jedis.set("ClassifyRuleChangeFlag", "false")
      //2.4：将数据重新的广播
      sc.broadcast(classifyRuleListUpdate)
    } else {
      ruleMapRef
    }
  }

  /**
    * 监视查询和预定解析规则是否发生了改变
    *
    * @param sc
    * @param queryAndBookRuleRef
    * @param jedis
    * @return
    */
  def monitorQueryAndBookRule(sc: SparkContext, queryAndBookRuleRef: Broadcast[mutable.HashMap[String, List[AnalyzeRule]]], jedis: JedisCluster): Broadcast[mutable.HashMap[String, List[AnalyzeRule]]] = {
    /**
      * 思路实现：
      * 1：获取redis里面的标记
      * 2：判断标记是否发生了改变
      *   2.1：重新获取数据库的过滤规则信息
      *   2.2：将当前广播变量的值进行删除
      *   2.3：将redis的标记重置
      *   2.4：将数据重新的广播
      * 3：如果标记没有发生改变
      * 将queryAndBookRuleRef直接返回
      */
    //1：获取redis里面的标记
    val needUpdateQueryAndBookRuleList = jedis.get("QueryAndBookRuleChangeFlag")

    //2：判断标记是否发生了改变
    //needUpdateQueryAndBookRuleList!=null: 判断redis的key是否存在
    //!needUpdateQueryAndBookRuleList.isEmpty : 判断redis的value值不是空
    if (needUpdateQueryAndBookRuleList != null && !needUpdateQueryAndBookRuleList.isEmpty && needUpdateQueryAndBookRuleList.toBoolean) {
      //2.1：重新获取数据库的过滤规则信息
      //查询规则数据
      val queryRuleListUpdate: List[AnalyzeRule] = AnalyzerRuleDB.queryRule(BehaviorTypeEnum.Query.id)
      //预定规则数据
      val bookRuleListUpdate: List[AnalyzeRule] = AnalyzerRuleDB.queryRule(BehaviorTypeEnum.Book.id)
      //将查询规则和预定规则放到map对象中进行广播
      val queryAndBookMap = new mutable.HashMap[String, List[AnalyzeRule]]()
      queryAndBookMap.put("QueryRule", queryRuleListUpdate)
      queryAndBookMap.put("BookRule", bookRuleListUpdate)
      //2.2：将当前广播变量的值进行删除
      queryAndBookRuleRef.unpersist()
      //2.3：将redis的标记重置
      jedis.set("QueryAndBookRuleChangeFlag", "false")
      //2.4：将数据重新的广播
      sc.broadcast(queryAndBookMap)
    } else {
      queryAndBookRuleRef
    }
  }

  /**
    * 监视黑名单ip是否发生了改变
    *
    * @param sc
    * @param ipBlackListRef
    * @param jedis
    * @return
    */
  def monitorIpBlackList(sc: SparkContext, ipBlackListRef: Broadcast[ArrayBuffer[String]], jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {
    /**
      * 思路实现：
      * 1：获取redis里面的标记
      * 2：判断标记是否发生了改变
      *   2.1：重新获取数据库的过滤规则信息
      *   2.2：将当前广播变量的值进行删除
      *   2.3：将redis的标记重置
      *   2.4：将数据重新的广播
      * 3：如果标记没有发生改变
      * 将ipBlackListRef直接返回
      */
    //1：获取redis里面的标记
    val needUpdateIpBlackList: String = jedis.get("IpBlackListChangeFlag")

    //2. 判断标记是否发生了改变
    //needUpdateIpBlackList!=null: 判断redis的key是否存在
    //!needUpdateIpBlackList.isEmpty : 判断redis的value值不是空
    if (needUpdateIpBlackList != null && !needUpdateIpBlackList.isEmpty && needUpdateIpBlackList.toBoolean) {
      //2.1. 重新获取数据库的过滤规则信息
      val ipBlackListUpdate: ArrayBuffer[String] = AnalyzerRuleDB.queryIpBlackList()
      //2.2. 将当前广播变量的值进行删除
      ipBlackListRef.unpersist()
      //2.3. 将redis的标记重置
      jedis.set("IpBlackListChangeFlag", "false")
      //2.4. 重新广播
      sc.broadcast(ipBlackListUpdate)
    } else {
      ipBlackListRef
    }
  }

}
