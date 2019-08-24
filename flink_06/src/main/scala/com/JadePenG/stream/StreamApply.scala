package com.JadePenG.stream

import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * apply方法实现单词统计
  * apply方法可以进行一些自定义处理，通过匿名内部类的方法来实现。当有一些复杂计算时使用。
  *
  * 用法
  * 1. 实现一个 WindowFunction 类
  * 2. 指定该类的泛型为 [输入数据类型, 输出数据类型, keyBy中使用分组字段的类型, 窗口类型]
  *
  * @author Peng
  */
object StreamApply {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 获取流处理运行环境
      * 2. 构建socket流数据源，并指定IP地址和端口号
      * 3. 对接收到的数据转换成单词元组
      * 4. 使用 keyBy 进行分流（分组）
      * 5. 使用 timeWindow 指定窗口的长度（每3秒计算一次）
      * 6. 实现一个WindowFunction匿名内部类
      * 在apply方法中实现聚合计算
      * 使用Collector.collect收集数据
      * 7. 打印输出
      * 8. 启动执行
      * 9. 在Linux中，使用 nc -lk 端口号 监听端口，并发送单词
      */
    //1.运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 构建socket数据源, 种地IO地址和端口号
    val socketDataStream: DataStream[String] = env.socketTextStream("node01", 9999)

    //3. 对接收到的数据转换成单词元组
    val wordAndOneData: DataStream[(String, Int)] = socketDataStream.flatMap(_.split(" ").map((_, 1)))
    //4. 使用 keyBy 进行分流（分组）  根据元组对象的第一个参数 _._1
    val groupDataStream: KeyedStream[(String, Int), String] = wordAndOneData.keyBy(_._1)
    //5. 使用 timeWindow 指定窗口的长度（每3秒计算一次）
    val windowDataStream: WindowedStream[(String, Int), String, TimeWindow] = groupDataStream.timeWindow(Time.seconds(3))
    //6. 实现一个WindowFunction匿名内部类
    val applyDataStream = windowDataStream.apply(new RichWindowFunction[(String, Int), (String, Int), String, TimeWindow] {

      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val reduceTuple: (String, Int) = input.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
        out.collect(reduceTuple)
      }
    })
    applyDataStream.print()

    env.execute()
  }
}
