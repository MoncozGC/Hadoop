package com.air.antispider.stream.dataprocess.businessprocess

import java.util

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.dataprocess.constants.BehaviorTypeEnum
import org.apache.commons.beanutils.PropertyUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD

/**
  * 数据推送
  *
  * @author Peng
  */
object DataSend {

  /**
    * 将查询请求的数据发送到查询的topic中
    *
    * @param processedRDD 处理完的每一条数据
    */
  def sendQueryDataToKafka(processedRDD: RDD[ProcessedData]): Unit = {
    /**
      * 实现思路:
      * 1. 过滤出哪些请求数据是query数据
      * 2. 判断query是否为空
      * 3. 将数据写入到kafka中
      *   3.1 读取topic
      *   3.2 创建map封装kafka参数
      *   3.3 设置brokerList
      *   3.4 kv的序列化
      *   3.5 设置批次发送
      *   3.6 创建分区按照分区发送
      *   3.7 发送数据
      *   3.8 关闭连接
      */
    //1：过滤出来哪些数据属于query数据
    val queryDataToKafkaStr: RDD[String] = processedRDD.filter(processed => processed.requestType.behaviorType == BehaviorTypeEnum.Query)
      .map(_.toKafkaString())
    //2. 判断query是否为空
    if (!queryDataToKafkaStr.isEmpty()) {
      //3. 将数据写入到kafka中
      //3.1 读取topic
      val topic: String = PropertiesUtil.getStringByKey("source.query.topic", "kafkaConfig.properties")
      //3.2 创建map封装kafka参数
      val props: util.HashMap[String, Object] = new util.HashMap[String, Object]()
      //3.3 设置brokerList
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
      //3.4 设置kv的序列化
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.key_serializer_class_config", "kafkaConfig.properties"))
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.value_serializer_class_config", "kafkaConfig.properties"))

      //3.5 设置批次发送[可以提高效率]  根据配置文件设置: 当数据满足32M大小,或者往kafka写入数据10毫秒就分一批次
      //一个批次提交数据大小
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, PropertiesUtil.getStringByKey("default.batch_size_config", "kafkaConfig.properties"))
      //往kafka服务器提交消息间隔时间，0则立即提交不等待
      props.put(ProducerConfig.LINGER_MS_CONFIG, PropertiesUtil.getStringByKey("default.linger_ms_config", "kafkaConfig.properties"))

      //3.6 创建分区按照分区发送数据 Driver端创建
      queryDataToKafkaStr.foreachPartition(partition => {
        //实例化一个生产者实例 保证每一个分区实例化一个对象  [如果写在foreach中那么每来一条数据就会实例化一个对象]
        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

        //循环partition的数据进行发送
        partition.foreach(record => {
          //3.7 发送数据  创建一个没有密钥的记录 ProducerRecord
          val pushRecord: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, record)
          producer.send(pushRecord)
        })
        //3.8 关闭连接
        producer.close()
      })
    }

  }

  /**
    * 将预定的数据发送到预定的topic中
    *
    * @param processedRDD 处理完的每一条数据
    */
  def sendBookDataToKafka(processedRDD: RDD[ProcessedData]) = {
    /**
      * 实现思路:
      * 1. 过滤出来哪些数据属于book数据
      * 2. 判断book请求数据是否为空
      * 3. 推送数据到kafka中
      *   3.1 创建topic
      *   3.2 创建map封装kafka参数
      *   3.3 设置brokerList
      *   3.4 设置kv的序列化
      *   3.5 设置批次发送
      *   3.6 创建分区按照分区发送
      *   3.7 发送数据
      *   3.8 关闭连接
      */
    //1. 过滤出来哪些数据属于book数据
    val bookDataToKafkaStr: RDD[String] = processedRDD.filter(processed => processed.requestType.behaviorType == BehaviorTypeEnum.Book).map(_.toKafkaString())
    //2. 判断book请求数据是否为空
    if (!bookDataToKafkaStr.isEmpty()) {
      //3. 推送数据到kafka中
      //3.1 创建topic
      val topic = PropertiesUtil.getStringByKey("source.book.topic", "kafkaConfig.properties")
      //3.2 创建map封装kafka参数
      val props = new util.HashMap[String, Object]()
      //3.3 设置brokerList
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
      //3.4 设置kv的序列化
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.key_serializer_class_config", "kafkaConfig.properties"))
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.value_serializer_class_config", "kafkaConfig.properties"))
      //3.5 设置批次发送
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, PropertiesUtil.getStringByKey("default.batch_size_config", "kafkaConfig.properties"))
      props.put(ProducerConfig.LINGER_MS_CONFIG, PropertiesUtil.getStringByKey("default.linger_ms_config", "kafkaConfig.properties"))

      //3.6 按照分区发送数据
      bookDataToKafkaStr.foreachPartition(partition => {
        //实例化一个生产者
        val product = new KafkaProducer[String, String](props)

        partition.foreach(record => {
          //3.7 发送数据
          val pushRecord = new ProducerRecord[String, String](topic, record)
          product.send(pushRecord)
        })
        //3.8 关闭连接
        product.close()
      })
    }


  }

}
