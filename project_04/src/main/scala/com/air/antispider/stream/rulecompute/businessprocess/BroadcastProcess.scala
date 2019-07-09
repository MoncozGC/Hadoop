package com.air.antispider.stream.rulecompute.businessprocess

import com.air.antispider.stream.common.bean.FlowCollocation
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

/**
  * 监视广播数据是否更新
  *
  * @author Peng
  */
object BroadcastProcess {


  /**
    * 监视关键页面
    *
    * @param sc                      sparkContext
    * @param queryCritcalPageListRef 广播到executor节点的关键页面规则
    * @param jedis                   数据需保存在redis
    * @return
    */
  def monitorQueryCritcalPageList(sc: SparkContext, queryCritcalPageListRef: Broadcast[ArrayBuffer[String]], jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {
    /**
      * 实现思路:
      * 1. 获取redis里面的标记
      * 2. 判断标记是否发生了改变
      *   2.1. 重新获取数据库的过滤规则信息
      *   2.2. 将当前广播变量的值进行删除
      *   2.3. 将redis的标记重置
      *   2.4. 将数据重新的广播
      * 3. 如果标记没有发生改变
      * 将queryCritcalPageListRef直接返回
      */
    //1. 获取redis里面的标记
    val needUpdateCritcalPageList = jedis.get("CritcalPageChangeFlag")
    //2. 判断标记是否发生了改变
    //needUpdateCritcalPageList!=null: 判断redis的key是否存在
    //!needUpdateCritcalPageList.isEmpty : 判断redis的value值不是空
    if (needUpdateCritcalPageList != null && !needUpdateCritcalPageList.isEmpty && needUpdateCritcalPageList.toBoolean) {
      //2.1. 重新获取数据库的过滤规则信息
      val criticalPageListUpdate: ArrayBuffer[String] = AnalyzerRuleDB.queryCriticalPageList()
      //2.2 将当前广播变量的值进行删除
      queryCritcalPageListRef.unpersist()
      //2.3. 将redis的标记重置
      jedis.set("CritcalPageChangeFlag", "false")
      //2.4. 将数据重新的广播
      sc.broadcast(criticalPageListUpdate)
    } else {
      queryCritcalPageListRef
    }
  }

  /**
    * 监视黑名单规则
    *
    * @param sc
    * @param ipBlackListRef
    * @param jedis
    * @return
    */
  def monitorIpBlackList(sc: SparkContext, ipBlackListRef: Broadcast[ArrayBuffer[String]], jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {
    /**
      * 实现思路:
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
    val needUpdateIpBlackList = jedis.get("IpBlackListChangeFlag")

    //2：判断标记是否发生了改变
    //needUpdateIpBlackList!=null: 判断redis的key是否存在
    //!needUpdateIpBlackList.isEmpty : 判断redis的value值不是空
    if (needUpdateIpBlackList != null && !needUpdateIpBlackList.isEmpty && needUpdateIpBlackList.toBoolean) {
      //2.1：重新获取数据库的过滤规则信息
      val ipBlackListUpdate = AnalyzerRuleDB.queryIpBlackList()
      //2.2：将当前广播变量的值进行删除
      ipBlackListRef.unpersist()
      //2.3：将redis的标记重置
      jedis.set("IpBlackListChangeFlag", "false")
      //2.4：将数据重新的广播
      sc.broadcast(ipBlackListUpdate)
    } else {
      ipBlackListRef
    }
  }

  /**
    * 监视流程规则广播变量是否发生了改变
    *
    * @param sc
    * @param flowListRef
    * @param jedis
    * @return
    */
  def monitorFlowList(sc: SparkContext, flowListRef: Broadcast[ArrayBuffer[FlowCollocation]], jedis: JedisCluster): Broadcast[ArrayBuffer[FlowCollocation]] = {
    /**
      * 实现思路:
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
    val needUpdateFlowList = jedis.get("FlowListChangeFlag")

    //2：判断标记是否发生了改变
    //needUpdateFlowList!=null: 判断redis的key是否存在
    //!needUpdateFlowList.isEmpty : 判断redis的value值不是空
    if (needUpdateFlowList != null && !needUpdateFlowList.isEmpty && needUpdateFlowList.toBoolean) {
      //2.1：重新获取数据库的过滤规则信息
      val flowListUpdate = AnalyzerRuleDB.queryFlow()
      //2.2：将当前广播变量的值进行删除
      flowListRef.unpersist()
      //2.3：将redis的标记重置
      jedis.set("FlowListChangeFlag", "false")
      //2.4：将数据重新的广播
      sc.broadcast(flowListUpdate)
    } else {
      flowListRef
    }
  }
}
