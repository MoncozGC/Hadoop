package com.JadePenG.stream

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * 接收socket的单词数量, 进行单词统计
  *
  * @author Peng
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    /**
      * 1.运行环境
      * 2. 构建socket数据源, 种地IO地址和端口号
      * 3. 对接收到的数据转换成单词元组对象
      * 4. 受用keyBy进行分流
      * 5. 使用timeWindow指定窗口的长度和滑动距离
      * 6. 使用sum进行累加数据
      * 7. 打印输出
      */
    //1.运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 构建socket数据源, 种地IO地址和端口号
    val socketDataStream = env.socketTextStream("node01", 9999)

    //3. 对接收到的数据转换成单词元组对象
    val wordAndOneDataStream = socketDataStream.flatMap(_.split(" ")).map(_ -> 1)

    //4. 受用keyBy进行分流
    //将相同的key放在一起 按照指定的key来进行分流，类似于批处理中的group By。
    val groupDataStream = wordAndOneDataStream.keyBy((0))

    //5. 使用timeWindow指定窗口的长度和滑动距离
    //TimeWindow: 有两个方法 一个只有窗口的长度, 一个既有窗口长度也有滑动距离
    //每5秒统计过去2秒钟的数据 --> 丢失数据
    //每2秒钟统计过去5秒的数据 --> 重复
    //val windowDataStream = groupDataStream.timeWindow(Time.seconds(2), Time.seconds(2))

    //countWindow 每3条数据(分流之后的)统计过去5条数据
    //hadoop spark spark  这是三条数据因为数据进行 空格 切分过
    val windowDataStream = groupDataStream.countWindow(5, 3)

    val reduceDataStream = windowDataStream.sum(1)

    reduceDataStream.print()

    env.execute("StreamWordCount")
  }

}
