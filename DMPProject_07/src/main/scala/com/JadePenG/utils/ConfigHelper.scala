package com.JadePenG.utils

import com.typesafe.config.{ConfigFactory, ConfigValue, ConfigValueType}
import org.apache.spark.sql.SparkSession

class ConfigHelper(builder: SparkSession.Builder) {

  /**
    * 问题: 每次都去取 很烦
    * @return
    */
  def loadConfig(): SparkSession.Builder = {
    import scala.collection.JavaConverters._
    // 1. 加载配置, 获取所有的配置项
    val config = ConfigFactory.load("spark")
    val kvSet = config.entrySet()
    // 2. 遍历配置项
    for (kv <- kvSet.asScala) {
      // 3. 获取 Key 和 Value
      val key = kv.getKey
      // 问题: 怎么拿到值
      val value: ConfigValue = kv.getValue

      // 问题: 所有 kv 包含了环境变量
      // 判断是否是 String 类型
      if (value.origin().filename() != null && value.valueType() == ConfigValueType.STRING) {
        val rvalue = value.unwrapped().asInstanceOf[String]
        builder.config(key, rvalue)
      }
    }
    builder
  }
}

object ConfigHelper {
  private val name: String = ""

  // 把 builder 转成 ConfigHelper
  implicit def builderToHelper(builder: SparkSession.Builder): ConfigHelper = {
    new ConfigHelper(builder)
  }
}
