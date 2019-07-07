package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean._
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum

/**
  * 数据格式化
  *
  * @author Peng
  */
object DataPackage {
  /**
    * 将需要写入kafka的数据格式化(与ProcessedData对应),
    *
    * @param sourceData       原数据(kafka中未处理的数据, 也可以不传 冗余)
    * @param record           requestMethod(请求方法) request(请求路径) remoteAddr(客户端ip) cookieValue_JSESSIONID(cookie中的jessionid) cookieValue_USERID(cookie中的userid)
    * @param highFreIp        高频ip
    * @param requestTypeLabel 数据分类标签
    * @param travelTypeLabel  单程往返标签
    * @param queryRequestData 解析的查询数据
    * @param bookRequestData  解析的预定数据
    */
  def dataPackage(sourceData: String, record: AccessLog, highFreIp: Boolean, requestTypeLabel: RequestType, travelTypeLabel: TravelTypeEnum.Value,
                  queryRequestData: Option[QueryRequestData], bookRequestData: Option[BookRequestData]) = {

    /**
      * 改方法就是为了构建核心数据: requestParams: CoreRequestParams还没有, 所以需要封装
      * 用于封装核心请求信息：飞行时间、目的地、出发地  数据来自于QueryRequestData(查询) BookRequestData(预定)类
      * case class CoreRequestParams(flightDate: String, depCity: String, arrCity: String)
      */

    //飞行时间
    var flightDate: String = ""

    //在预定里面拿数据
    bookRequestData match {
      //flightDate是ListBuffer类型, 需要转换成String类型 mkString
      case Some(book) => flightDate = book.flightDate.mkString
      //匹配不上就不做操作, 默认也是 ""(空的字符串)
      // 不可以写None => flightDate = "", 因为在预定中如果已经拿到了数据(匹配上了),
      // 然后又进入查询的模式匹配中, 匹配不上, 然后经过 None => flightDate = ""将flightDate之前的值覆盖掉了成了一个"", 这样就有问题!
      case None =>
    }

    //在查询里面拿数据
    queryRequestData match {
      case Some(query) => flightDate = query.flightDate.mkString
      case None =>
    }

    //出发地
    var arrCity: String = ""
    //在预定里面拿数据
    bookRequestData match {
      case Some(book) => arrCity = book.arrCity.mkString
      case None =>
    }

    //在查询里面拿数据
    queryRequestData match {
      case Some(query) => arrCity = query.arrCity.mkString
      case None =>
    }

    //目的地
    var depCity: String = ""
    //在预定里面拿数据
    bookRequestData match {
      case Some(book) => depCity = book.depCity.mkString
      case None =>
    }

    //在查询里面拿数据
    queryRequestData match {
      case Some(query) => depCity = query.depCity.mkString
      case None =>
    }

    //构建封装核心的请求参数
    val requestParams = CoreRequestParams(flightDate, depCity, arrCity)

    //构建结构化的数据所需要的参数  ProcessedData
    ProcessedData(sourceData, record.requestMethod, record.request, record.remoteAddr, record.httpUserAgent, record.timeIso8601,
      record.serverAddr, highFreIp, requestTypeLabel, travelTypeLabel, requestParams, record.jessionID, record.userID,
      queryRequestData, bookRequestData, record.httpReferer)
  }

}
