package com.air.antispider.stream.dataprocess.businessprocess


import java.util.regex.Pattern

import com.air.antispider.stream.common.util.decode.MD5
import com.air.antispider.stream.common.util.string.StringUtil

/**
  * 数据脱敏工具类
  *
  * @author Peng
  */
object EncryptionData {
  /**
    * 手机号脱敏
    *
    * @param httpCookie filterRDD中的cookie信息里面包含了手机号身份证号等敏感信息
    * @return
    */
  def encryptionPhone(httpCookie: String): String = {
    /**
      * 实现思路:
      * 1. 定义一个MD5加密类
      * 2. 定义手机号的正则表达式
      * 3. 匹配手机号
      * 4. 将手机号的前一位与后一位进行判断
      * 5. 判断手机号的前一位是否是数字, 然后判断后一位
      */
    //1：定义一个md5的加密类
    val md5 = new MD5()
    //复制给局部变量
    var encrypted = httpCookie
    //手机号正则
    val phonePattern = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\\d{8}")
    //匹配规则：java.util.regex.Matcher[pattern=((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\d{8} region=0,1440 lastmatch=]
    var phoneMatcher = phonePattern.matcher(encrypted)
    //查找手机号
    while (phoneMatcher.find()) {
      //手机号的前一个index，注：phoneMatcher.group()找到的手机号
      val lowIndex = phoneMatcher.start() - 1
      //手机号的前一个字符
      val lowLetter = encrypted.charAt(lowIndex).toString()
      //手机号的后一个index
      val highIndex = phoneMatcher.end()
      //如果前一位字符不是数字，那就要看后一位是否是数字
      if (!lowLetter.matches("^[0-9]$")) {
        //如果字符串的最后是手机号，直接替换即可
        if (highIndex < encrypted.length()) {
          //手机号的后一个字符
          val highLetter = encrypted.charAt(highIndex).toString()
          //后一位也不是数字，那说明这个字符串就是一个电话号码
          if (!highLetter.matches("^[0-9]$")) {
            encrypted = StringUtil.replace(encrypted, phoneMatcher.start(), phoneMatcher.end(), md5.getMD5ofStr(phoneMatcher.group()))
            phoneMatcher = phonePattern.matcher(encrypted) //更新matcher对象，重新匹配
          }
        } else {
          encrypted = StringUtil.replace(encrypted, phoneMatcher.start(), phoneMatcher.end(), md5.getMD5ofStr(phoneMatcher.group()))
          phoneMatcher = phonePattern.matcher(encrypted) //更新matcher对象，重新匹配
        }
      }
    }
    encrypted
  }

  /**
    * 身份证号码的加密
    *
    * @param httpCookie
    * @return
    */
  def encryptionID(httpCookie: String): String = {
    /**
      * 实现思路:
      * 1：定义一个md5的加密类
      * 2：定义身份证号码的正则表达式
      * 3：匹配身份证号码
      * 4：拿到身份证号码的前一位和后一位进行判断
      * 5：判断身份证号码的前一位是否是数字，然后判断手机号码的后一位
      */
    //1：定义一个md5的加密类
    val md5 = new MD5()
    //复制给局部变量
    var encrypted = httpCookie
    //身份证号正则
    val idPattern = Pattern.compile("(\\d{18})|(\\d{17}(\\d|X|x))|(\\d{15})")
    //匹配规则：java.util.regex.Matcher[pattern=((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\d{8} region=0,1440 lastmatch=]
    var idMatcher = idPattern.matcher(encrypted)
    //查找身份证号
    while (idMatcher.find()) {
      //身份证号的前一个index，注：phoneMatcher.group()找到的身份证号
      val lowIndex = idMatcher.start() - 1
      //身份证号的前一个字符
      val lowLetter = encrypted.charAt(lowIndex).toString()
      //身份证号的后一个index
      val highIndex = idMatcher.end()
      //如果前一位字符不是数字，那就要看后一位是否是数字
      if (!lowLetter.matches("^[0-9]$")) {
        //如果字符串的最后是身份证号，直接替换即可
        if (highIndex < encrypted.length()) {
          //身份证号的后一个字符
          val highLetter = encrypted.charAt(highIndex).toString()
          //后一位也不是数字，那说明这个字符串就是一个身份证号
          if (!highLetter.matches("^[0-9]$")) {
            encrypted = StringUtil.replace(encrypted, idMatcher.start(), idMatcher.end(), md5.getMD5ofStr(idMatcher.group()))
            idMatcher = idPattern.matcher(encrypted) //更新matcher对象，重新匹配
          }
        } else {
          encrypted = StringUtil.replace(encrypted, idMatcher.start(), idMatcher.end(), md5.getMD5ofStr(idMatcher.group()))
          idMatcher = idPattern.matcher(encrypted) //更新matcher对象，重新匹配
        }
      }
    }
    encrypted
  }

}
