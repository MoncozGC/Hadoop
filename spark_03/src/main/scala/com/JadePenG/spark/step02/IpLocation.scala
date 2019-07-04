package com.JadePenG.spark.step02

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 利用spark的广播变量实现ip地址查询
  */
object IpLocation {
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

  /**
    * 根据ip地址查找对应的经纬度信息
    * 查找到经纬度信息以后，返回该经纬度对应的访问次数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //创建sparkContext对象
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //读取城市ip地址库信息
    val cityIps = sc.textFile("spark_03\\src\\main\\resources\\scala\\step03\\IpLocation\\ip.txt").map(x => x.split("\\|")).map(x => (x(2), x(3), x(x.length - 2), x(x.length - 1)))
    //读取运营商日志信息
    val logs: RDD[String] = sc.textFile("spark_03\\src\\main\\resources\\scala\\step03\\IpLocation\\20090121000132.394251.http.format").map(x => x.split("\\|")(1))

    //广播
    val cityIpsList = cityIps.collect()
    val broadcastIpsRef = sc.broadcast(cityIpsList)

    val result: RDD[((String, String), Int)] = logs.mapPartitions(iter => {
      val broadcastValue = broadcastIpsRef.value

      iter.map(ip => {
        val ipNum = ip2Long(ip)
        val index = binarySearch(ipNum, broadcastValue)

        if (index == -1) {
          (("", ""), 0)
        } else {
          ((broadcastValue(index)._3, broadcastValue(index)._4), 1)
        }
      })
    })

    val finalResult = result.reduceByKey(_ + _)
    finalResult.foreach(println(_))

    sc.stop()
  }

}
