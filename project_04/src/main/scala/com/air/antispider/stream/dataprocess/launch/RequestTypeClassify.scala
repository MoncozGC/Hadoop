package com.air.antispider.stream.dataprocess.launch

import com.air.antispider.stream.common.bean.{AccessLog, RequestType}
import com.air.antispider.stream.dataprocess.constants.BehaviorTypeEnum.BehaviorTypeEnum
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}
import com.air.antispider.stream.dataprocess.constants.FlightTypeEnum.FlightTypeEnum

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 数据分类打标签
  *
  * @author Peng
  */
object RequestTypeClassify {
  /**
    * 数据分类打标签
    *
    * @param record  过来的一条数据 26/Jun/2019:03:42:54 -0800#CS#POST /B2C40/query/jaxb/direct/query.ao HTTP/1.1#CS#POST#CS#
    * @param ruleMap 广播变量的规则
    */
  def classifyByRequest(record: AccessLog, ruleMap: mutable.HashMap[String, ArrayBuffer[String]]): RequestType = {
    /**
      * 实现思路:
      * 1. 将广播变量的规则拿出来
      * 2. 判断当前的request请求跟哪个分类规则匹配, 然后打上对应的标签
      *
      * /B2C40/query/jaxb/direct/query.ao 请求参数
      */
    //1.将广播变量规则取出来
    //国内查询规则
    val nqList: ArrayBuffer[String] = ruleMap.getOrElse("NationalQueryList", null)
    //国际查询规则
    val iqList: ArrayBuffer[String] = ruleMap.getOrElse("InternationalQueryList", null)
    //国内查询规则
    val nbList: ArrayBuffer[String] = ruleMap.getOrElse("NationalBookList", null)
    //国际查询规则
    val ibList: ArrayBuffer[String] = ruleMap.getOrElse("InternationalBookList", null)

    //返回数据类型(国内/国际, 查询/预定)
    var requestType: RequestType = null

    //是否匹配上了规则
    var flag = false

    //匹配国内查询
    nqList.foreach(rule => {
      //获取一条数据中的request请求与分类规则进行匹配, 匹配成功打上对应的标签
      if (record.request.matches(rule) && !flag) {
        flag = true
        requestType = RequestType(FlightTypeEnum.National, BehaviorTypeEnum.Query)
      }
    })

    //匹配国际查询
    iqList.foreach(rule => {
      if (record.request.matches(rule) && !flag) {
        flag = true
        requestType = RequestType(FlightTypeEnum.International, BehaviorTypeEnum.Query)
      }
    })

    //匹配国内预定
    nqList.foreach(rule => {
      if (record.request.matches(rule) && !flag) {
        flag = true
        requestType = RequestType(FlightTypeEnum.National, BehaviorTypeEnum.Book)
      }
    })

    //匹配国际预定
    iqList.foreach(rule => {
      if (record.request.matches(rule) && !flag) {
        flag = true
        requestType = RequestType(FlightTypeEnum.International, BehaviorTypeEnum.Book)
      }
    })

    //没有匹配上任何规则
    if (!flag) {
      requestType = RequestType(FlightTypeEnum.Other, BehaviorTypeEnum.Other)
    }

    //返回打好的标签
    requestType
  }

}
