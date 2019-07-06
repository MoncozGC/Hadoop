package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.AccessLog
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

/**
  * 链路统计功能的实现
  *
  * @author Peng
  */
object BusinessProcess {


  def linkCount(logRDD: RDD[AccessLog], jedis: JedisCluster): Unit = {
    /**
      * 实现思路:
      * 1. 统计各个链路的数据采集量
      * 2. 统计各个链路的活跃连接数
      * 3. 将统计好的数据写入到redis集群
      **/
    //1. 统计各个链路的数据采集数
    val serverCount: RDD[(String, Int)] = logRDD.map(record => {
      /*val filed = record.split("#CS#")
      //获取ip
      val server_addr = filed(9)
      (server_addr, 1)*/
      (record.serverAddr, 1)
      //聚合
    }).reduceByKey(_ + _)

    //2. 将各个链路的活跃连接数
    val activeNum: RDD[(String, Int)] = logRDD.map(record => {
      /* val filed = record.split("#CS#")
       val server_addr = filed(9)
       //获取活跃连接数, 但是需要获取最新的活跃连接数
       val connection_num = filed(11)
       (server_addr, connection_num)*/
      (record.serverAddr, record.connectionsActive)
      // x: 临时累加值  y: 最新的活跃连接数 需要与指定的值进行比较的数据
    }).reduceByKey((x, y) => y)

    //3. 将统计好的数据写入到redis集群中  rdd数据现在在Driver端
    if (!serverCount.isEmpty() && !activeNum.isEmpty()) {
      //将计算好的RDD转换成map
      val serverCountMap: collection.Map[String, Int] = serverCount.collectAsMap()
      val activeNumMap = activeNum.collectAsMap()

      //将数据写入redis 包装到一个map对象中, 进行序列化成字符串
      val fieldsMap: Map[String, collection.Map[String, Int]] = Map(
        "serverCountMap" -> serverCountMap,
        "activeNumMap" -> activeNumMap
      )

      //获取到链路统计的key  为了防止不一直写入到一个key中,在key的名字上加一个时间戳
      val keyName = PropertiesUtil.getStringByKey("cluster.key.monitor.linkProcess", "jedisConfig.properties") + System.currentTimeMillis().toString
      //获取链路统计的超时时间 设置的过期时间, 否则redis中会有大量的key
      val expTime = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt

      //将写入redis的数据转换成String类型
      val value: String = Json(DefaultFormats).write(fieldsMap)

      //写入redis的值 setex: 带有过去时间的时间戳
      jedis.setex(keyName, expTime, value)
    }

  }

}
