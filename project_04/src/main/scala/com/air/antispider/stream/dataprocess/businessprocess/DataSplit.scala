package com.air.antispider.stream.dataprocess.businessprocess

import java.util.regex.Pattern

import com.air.antispider.stream.common.bean.AccessLog
import com.air.antispider.stream.common.util.decode.{EscapeToolBox, RequestDecoder}
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable


/**
  * 数据拆分功能
  *
  * @author Peng
  */
object DataSplit {
  /**
    * 将kafka消费出来的字符串转换成bean对象，方便后续使用
    */
  def parseAccessLog(rdd: RDD[String]): RDD[AccessLog] = {
    rdd.map(recode => {
      val fields = recode.split("#CS#")

      //    local message = time_local .."#CS#".. request .."#CS#".. request_method .."#CS#".. content_type .."#CS#".. request_body .."#CS#".. http_referer .."#CS#".. remote_addr .."#CS#"..
      //    http_user_agent .."#CS#".. time_iso8601 .."#CS#".. server_addr .."#CS#".. http_cookie .. "#CS#" .. ngx.var.connections_active;
      val Array(timeLocal, request, requestMethod, contentType, requestBody, httpReferer, remoteAddr, httpUserAgent,
      timeIso8601, serverAddr, httpCookie, connectionsActive) = fields

      //提取cookie信息,将里面的kv键值对放到map对象中
      val cookieMap = {
        var tempMap = new mutable.HashMap[String, String]()
        if (!httpCookie.equals("")) {
          httpCookie.split(";").foreach(s => {
            //s就是一个一个的键值对,
            val kv = s.split("=")
            if (kv.length > 1) {
              try {
                val chPattern = Pattern.compile("u([0-9a-fA-F]{4})")
                val chMatcher = chPattern.matcher(kv(1))
                var isUnicode = false
                while (chMatcher.find()) {
                  isUnicode = true
                }
                if (isUnicode) {
                  tempMap += (kv(0) -> EscapeToolBox.unescape(kv(1)))
                } else {
                  tempMap += (kv(0) -> RequestDecoder.decodePostRequest(kv(1)))
                }
              } catch {
                case e: Exception => e.printStackTrace()
              }
            }
          })
        }
        tempMap
      }

      //在cookie中获取jsessionid和userId4logCookie
      val cookie_jessionid = PropertiesUtil.getStringByKey("cookie.JSESSIONID.key", "cookieConfig.properties")
      val cookie_userid = PropertiesUtil.getStringByKey("cookie.userId.key", "cookieConfig.properties")

      //获取value
      val cookieValue_jessionid = cookieMap.getOrElse(cookie_jessionid, "NULL")
      val cookieValue_userid = cookieMap.getOrElse(cookie_userid, "NULL")

      AccessLog(timeLocal, request, requestMethod, contentType, requestBody, httpReferer, remoteAddr, httpUserAgent,
        timeIso8601, serverAddr, httpCookie, connectionsActive.toInt, cookieValue_jessionid, cookieValue_userid)

    })
  }

}
