package com.air.antispider.stream.common.bean

//case class与class的区别
//1：不需要使用new
//2:  默认实现了序列化
//3:  可以使用模式匹配
/**
  * 日志
  * @param timeLocal
  * @param request
  * @param requestMethod
  * @param contentType
  * @param requestBody
  * @param httpReferer
  * @param remoteAddr 客户端服务器ip
  * @param httpUserAgent
  * @param timeIso8601
  * @param serverAddr  服务端服务器ip
  * @param httpCookie
  * @param connectionsActive
  */
case class AccessLog(timeLocal:String,
                     request:String,
                     requestMethod:String,
                     contentType:String,
                     requestBody:String,
                     httpReferer:String,
                     remoteAddr:String,
                     httpUserAgent:String,
                     timeIso8601:String,
                     serverAddr:String,
                     var httpCookie:String,
                     connectionsActive:Int,
                     jessionID:String,
                     userID:String
                    )
