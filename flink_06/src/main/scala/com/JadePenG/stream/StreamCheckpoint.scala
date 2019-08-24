package com.JadePenG.stream

import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * 添加一个source，每秒生成一个订单数据（订单ID、用户ID、订单金额），分别计算出每个用户的订单总金额。要求
  * 使用checkpoint将用户的总金额进行快照。
  *
  * @author Peng
  */
object StreamCheckpoint {

  //3. 创建一个订单样例类（订单ID、用户ID、订单金额）
  case class Order(id: String, userName: String, money: Long)

  //要求传递一个泛型类型, 但是Long没有继承序列化接口  所有使用case class 用来保存checkpoint的总金额,
  case class UDFState(totalMoney: Long)

  def main(args: Array[String]): Unit = {
    /**
      * 1. 创建流处理环境
      * 2. 配置checkpoint相关参数
      * 3. 创建一个订单样例类（订单ID、用户ID、订单金额）
      * 4. 添加一个添加自动生成订单数据的source
      * 5. 使用 keyBy 按照用户名分流
      * 6. 使用 timeWindow 划分时间窗口
      * 7. 使用 apply 进行自定义计算
      * 8. 运行测试
      * 9. 查看HDFS中是否已经生成checkpoint数据
      */
    //1. 创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 配置checkpoint相关参数
    //每1s开启一个checkpoint
    env.enableCheckpointing(1000)
    //设置checkpoint的快照路径
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/myTest/flink/checkpoint"))
    //设置执行模式, 最多执行一次还是至少执行一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置超时时间
    env.getCheckpointConfig.setCheckpointTimeout(6000)
    //设置同一时间有多少个checkpoint可以同时执行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //4. 添加一个添加自动生成订单数据的source
    val orderDataStream: DataStream[Order] = env.addSource(new RichSourceFunction[Order] {
      //记录当前是否需要继续生成数据
      var isRunning: Boolean = true

      //生成数据
      override def run(sourceContext: SourceFunction.SourceContext[Order]): Unit = {
        while (isRunning) {
          //生成一个订单数据 (订单id, 用户名, 金额)  统计每个用户的总金额  用户名要重复多的才更好统计
          val order = Order(UUID.randomUUID().toString, Random.nextInt(3).toString, Random.nextInt(100))

          //将数据收集返回  只收集一次,但是我们需要的是一直收集
          sourceContext.collect(order)

          //休眠1秒  线程安全
          TimeUnit.SECONDS.sleep(1)
        }
      }

      //取消操作的时候执行. 程序宕机或者kill的时候执行
      override def cancel(): Unit = {
        //false不再生成数据
        isRunning = false
      }
    })

    //5. 使用 keyBy 按照用户名分流
    val groupDataStream: KeyedStream[Order, String] = orderDataStream.keyBy(_.userName)

    //6. 使用 timeWindow 划分时间窗口
    val windowDataStream: WindowedStream[Order, String, TimeWindow] = groupDataStream.timeWindow(Time.seconds(5))
    //val resultDataStream: DataStream[Order] = windowDataStream.reduce((o1, o2) => Order(o1.id, o1.userName, o1.money + o2.money))

    //resultDataStream.print()
    //env.execute()

    //7. 使用 apply 进行自定义计算   ListCheckpointed 泛型就是写入的数据  是序列化的,所有构建一个case class
    //Order传入 Long返回 String: key  TimeWindow时间划分
    val resultDataStream: DataStream[Long] = windowDataStream.apply(new RichWindowFunction[Order, Long, String, TimeWindow] with ListCheckpointed[UDFState] {
      //总金额
      var totalMoney: Long = 0

      //使用apply方式自定义计算, 再apply方法中进行累加
      override def apply(key: String, window: TimeWindow, input: Iterable[Order], out: Collector[Long]): Unit = {
        //累加
        //input.reduce()
        for (elem <- input) {
          totalMoney = totalMoney + elem.money
        }
        out.collect(totalMoney)
      }

      //快照 需要实现快照功能,存储数据
      override def snapshotState(l: Long, l1: Long): util.List[UDFState] = {
        //将当前的计算总金额封装到UDFState
        val udfState: UDFState = UDFState(totalMoney)
        //需要返回一个List
        val states: util.ArrayList[UDFState] = new util.ArrayList[UDFState]()
        //添加
        states.add(udfState)
        //返回
        states
        //如何存储的是底层实现的
      }

      //恢复快照  程序出了错误就从快照中恢复数据
      override def restoreState(state: util.List[UDFState]): Unit = {
        //找第一个数据  14:42
        val udfState: UDFState = state.get(0)
        totalMoney = udfState.totalMoney
      }
    })
    //打印测试
    resultDataStream.print()

    //执行任务
    env.execute()
  }
}
