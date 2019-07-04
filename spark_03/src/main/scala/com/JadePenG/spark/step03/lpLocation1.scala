package com.JadePenG.spark.step03

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 根据ip地址查找对应的经纬度信息
  * 查找到经纬度信息以后，返回该经纬度对应的访问次数
  *
  * @author Peng
  */
object lpLocation1 {

  //将ip地址转换为Long类型   192.168.200.100
  def ip2Long(ip: String): Long = {
    val ips: Array[String] = ip.split("\\.")
    var ipNum: Long = 0L
    //遍历数组
    for (i <- ips) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  //利用二分查询，找到long类型数字在数组中的下标     ip开始数字  ip结束数字   经度    维度
  def binarySearch(ipNum: Long, broadcastValue: Array[(String, String, String, String)]): Int = {
    var start = 0
    var end = broadcastValue.length - 1
    while (start <= end) {
      val middle = (start + end) / 2

      if (ipNum >= broadcastValue(middle)._1.toLong && ipNum <= broadcastValue(middle)._2.toLong) {
        return middle
      }
      if (ipNum < broadcastValue(middle)._1.toLong) {
        end = middle - 1
      }
      if (ipNum > broadcastValue(middle)._2.toLong) {
        start = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    //读取城市ip地址库信息
    val cityIps: RDD[(String, String, String, String)] = sc.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_Spark RDD\\资料\\服务器访问日志根据ip地址查找区域\\用户ip上网记录以及ip字典\\ip.txt")
      .map(x => x.split("\\|")).map(x => (x(2), x(3), x(x.length - 2), x(x.length - 1)))
    //读取运营商日志信息(2), x(3), x(x.length - 2), x(x.length - 1)))
    val logs: Dataset[String] = spark.read.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_Spark RDD\\资料\\服务器访问日志根据ip地址查找区域\\用户ip上网记录以及ip字典\\20090121000132.394251.http.format")


    import spark.implicits._
    //指定列名
    val ipRulesDF: DataFrame = cityIps.toDF("start_num", "end_num", "longitude", "latitude")

    //将ip规则信息注册成视图
    ipRulesDF.createOrReplaceTempView("v_ipRules")

    //整理访问日志, 指定列名名
    val ipLogs = logs.map(line => {
      val fields = line.split("\\|")
      val ip = fields(1)

      ip2Long(ip)
    }).toDF("ip")

    //将log注册成视图
    ipLogs.createOrReplaceTempView("v_logs")

    //便携sql风格语法
    spark.sql(
      """
        |select longitude,latitude, count(1) counts
        | from v_ipRules a, v_logs b
        | where b.ip >= a.start_num and b.ip <= a.end_num
        | group by longitude,latitude
        | order by counts desc
      """.stripMargin).show()

    //释放资源
    spark.stop()


  }

}
