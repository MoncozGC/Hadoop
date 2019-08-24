package com.JadePenG.dmp

import com.JadePenG.utils.ConfigHelper
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
  *
  * @author Peng
  */
class HOCONTest {
  @Test
  def HoCont(): Unit = {
    val config = ConfigFactory.load("hocon")
    val bar = config.getInt("foo.bar")
    val baz = config.getInt("foo.baz")
    println(bar, baz)
  }

  @Test
  def sparkUse(): Unit = {
    //这种形式加载配置文件, 会在引用类文件中写一堆, 工具类中写一堆  意义不大
    /*val spark = SparkSession.builder()
      .appName("config")
      .master("local[6]")
      .config("spark.cores.max", ConfigHelper.str)
      .getOrCreate()*/

    //进行优化
    /**
      * 1. master会返回一个builder对象
      * 2. 将builder转换成ConfigHelper
      * 3. 对ConfigHelper进行处理, 并且将ConfigHelper转换回来成builder
      * 4. 调用getOrCreate()
      */

    Logger.getLogger("org").setLevel(Level.WARN)

    import ConfigHelper._
    val spark: SparkSession = SparkSession.builder()
      .appName("config")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    //打印配置文件中的信息
    println(spark.conf.getAll)
  }

}
