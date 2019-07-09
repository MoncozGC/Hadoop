package com.air.antispider.stream.common.bean

/**
  * 流程类：对应表nh_process_info
  * （规则配置集合和阈值参数）
  *
  * @param flowId         流程ID
  * @param flowName       流程名字
  * @param rules          流程的八项规则
  * @param flowLimitScore 阈值(当用户打分超过这个值就是爬虫ip)
  * @param strategyCode   策略Code(直接使用流程ID代替)
  */
case class FlowCollocation(
                            flowId: String,
                            flowName: String,
                            rules: List[RuleCollocation],
                            flowLimitScore: Double = 100,
                            strategyCode: String)
