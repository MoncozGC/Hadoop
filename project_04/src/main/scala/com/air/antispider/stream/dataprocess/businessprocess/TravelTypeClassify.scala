package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum

/**
  * 对数据进行单程或者往返打标签
  *
  * @author Peng
  */
object TravelTypeClassify {
  /**
    * 对数据进行单程或者往返打标签
    *
    * @param httpReferer url
    */
  def classifyByReferer(httpReferer: String): TravelTypeEnum.Value = {
    /**
      * 实现思路:
      * 1. 定义日期格式的正则表达式
      * 2. 定义一个变量保存日期的个数
      * 3. 对referer请求进行拆分
      * 4. 获取日期的数量
      * 5. 根据日期数据打标签
      * 6. 返回标签
      */
    //1. 定义日期格式的正则表达式
    val regex = "^(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])$"
    //2. 定义一个变量保存日期的个数
    var dayCount = 0

    //有参数拼接, 并且根据 "?"问好 切割长度>2才进行判断
    if (httpReferer.contains("?") && httpReferer.split("\\?").length > 1) {
      //3. 对referer请求进行拆分  先根据 "?" 拆分获取后面的参数, 然后根据 "&" 拆分获取拼接的参数
      val params: Array[String] = httpReferer.split("\\?")(1).split("&")
      //循环kv键值对, 查找日期
      for (parme <- params) {
        //d1=2019-07-11
        val kv = parme.split("=")
        //如果切割的字符串长度大于2, 并且 "=" 后面的参数匹配上了日期的正则表达式  那么就对保存日期的个数+1(相当于打个标签在下面进行判断返回)
        if (kv.length > 1 && kv(1).matches(regex)) {
          dayCount = dayCount + 1
        }
      }
    }

    //返回标签
    if (dayCount == 2) {
      //往返
      TravelTypeEnum.RoundTrip
    } else if (dayCount == 1) {
      //单程
      TravelTypeEnum.OneWay
    } else {
      //未知
      TravelTypeEnum.Unknown
    }
  }

}
