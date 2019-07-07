package com.air.antispider.stream.dataprocess.businessprocess

import scala.collection.mutable.ArrayBuffer

/**
  * 数据加工
  *
  * @author Peng
  */
object IpOperation {
  /**
    * 判断当前ip是否是黑名单ip
    *
    * @param remoteAddr  客户端服务器ip
    * @param ipBlackList 广播到executor节点的黑名单ip
    */
  def ipFreIp(remoteAddr: String, ipBlackList: ArrayBuffer[String]): Boolean = {
    /**
      * 实现思路:
      * 1. 设置一个变量标记是否是高频ip
      * 2. 循环判断数据库中黑名单ip跟当前ip是否匹配，如果匹配就是高频
      */
    var flag = false

    ipBlackList.foreach(ipBlack => {
      if (ipBlackList.eq(remoteAddr)) {
        flag = true
      }
    })
    flag
  }

}
