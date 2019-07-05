package com.air.antispider.stream.dataprocess.businessprocess

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

/**
  * 广播变量监视类
  *
  * @author Peng
  */
object BroadcastProcess {
  /**
    * 监视过滤规则是否发生了改变
    *
    * @param sc            用来重新广播
    * @param filterRuleRef 获取到数据,用来广播的
    * @param jedis         获取到redis中的标记
    * @return
    */
  def monitorFilterRule(sc: SparkContext, filterRuleRef: Broadcast[ArrayBuffer[String]], jedis: JedisCluster): Broadcast[ArrayBuffer[String]] = {
    /**
      * 思路实现:
      * 1. 获取redis里面的标记
      * 2. 判断标记是否发生了改变
      *  2.1 重新获取数据库的过滤规则信息
      *  2.2 将当前广播变量的值进行删除
      *  2.3 将redis的标记重置
      *  2.4 将数据重新的广播
      * 3. 如果标记没有发生改变 将filterRuleRef直接返回
      */
    //1. 获取redis里面的标记
    val needUpdateFilterRuleList = jedis.get("FilterRuleChangeFlag")

    //2. 判断标记是否发生了改变
    //needUpdateFilterRuleList != null  判断redis的key是否存在
    //needUpdateFilterRuleList.isEmpty  判断redis的value值不是空
    //needUpdateFilterRuleList.toBoolean  true 就是发生了改变
    if (needUpdateFilterRuleList != null && !needUpdateFilterRuleList.isEmpty && needUpdateFilterRuleList.toBoolean) {
      //2.1 重新获取数据库的规则信息
      val filterRuleListUpdate = AnalyzerRuleDB.queryFilterRule()
      //2.2 将当前广播变量的值进行删除
      filterRuleRef.unpersist()
      //2.3 将redis的标记重置
      jedis.set("FilterRuleChangeFlag", "false")
      //2.4 将数据重新的广播
      sc.broadcast(filterRuleListUpdate)
    } else {
      //3. 如果标记没有发生改变 将filterRuleRef直接返回
      filterRuleRef
    }
  }

}
