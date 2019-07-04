package com.JadePenG.spark.step03

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * sparkCore与SparkSql集成使用
  * @author Peng
  */
object lpLocation2 {
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
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()

    val sc = spark.sparkContext

    //读取城市ip地址库信息
    val cityIps: RDD[(String, String, String, String)] = sc.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_Spark RDD\\资料\\服务器访问日志根据ip地址查找区域\\用户ip上网记录以及ip字典\\ip.txt")
      .map(x => x.split("\\|")).map(x => (x(2), x(3), x(x.length - 2), x(x.length - 1)))
    //读取运营商日志信息(2), x(3), x(x.length - 2), x(x.length - 1)))
    val logs: Dataset[String] = spark.read.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_Spark RDD\\资料\\服务器访问日志根据ip地址查找区域\\用户ip上网记录以及ip字典\\20090121000132.394251.http.format")

    //将全部的ip规则收集到Driver端
    val ipRulesInDriver: Array[(String, String, String, String)] = cityIps.collect()
    //将ip信息广播出去, 线程是阻塞的 没有广播成功不会往下执行
    val ipRulesRef: Broadcast[Array[(String, String, String, String)]] = spark.sparkContext.broadcast(ipRulesInDriver)

    import spark.implicits._

    val ipLogs: DataFrame = logs.map(line => {
      val fields = line.split("\\|")
      val ip = fields(1)
      ip2Long(ip)
    }).toDF("ip")

    //将ip自定义成视图
    ipLogs.createOrReplaceTempView("v_logs")

    //定义自定义函数，自定义函数在哪里定义（Driver），在哪里执行的Exector
    spark.udf.register("nip", (ipNum: String) => {
      val rulesInExecutor: Array[(String, String, String, String)] = ipRulesRef.value
      val index = binarySearch(ipNum.toLong, rulesInExecutor)

      //-1代表没有找到 如果没有找到就返回空的字符串
      if (index == -1) {
        ""
      } else {
        rulesInExecutor(index)._3 + ":" + rulesInExecutor(index)._4
      }
    })

    //执行sql查询语句  as 重命名列名或者表名
    spark.sql(
      """
        |select nip(ip) as location, count(1) counts
        |from v_logs
        |group by location
        |order by counts desc
      """.stripMargin).show()

    spark.stop()
  }
}
