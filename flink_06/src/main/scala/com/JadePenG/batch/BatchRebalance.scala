package com.JadePenG.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * rebalance 会使用轮询的方式将数据均匀打散，这是处理数据倾斜最好的选择。
  * 将数据打散平均放到每个分区中
  *
  * @author Peng
  */
object BatchRebalance {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 构建批处理运行环境
      * 2. 使用 env.generateSequence 创建0-100的并行数据
      * 3. 使用 filter 过滤出来 大于8 的数字
      * 4. 使用map操作传入 RichMapFunction ，将当前子任务的ID和数字构建成一个元组
      * 在RichMapFunction中可以使用 getRuntimeContext.getIndexOfThisSubtask 获取子任务序号
      * 5. 打印测试
      */

    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    //2. 使用 env.generateSequence 创建0-100的并行数据
    val numDataSet: DataSet[Long] = env.generateSequence(0, 100)

    //3. 使用 filter 过滤出来 大于8 的数字
    val filterDataSet: DataSet[Long] = numDataSet.filter(_> 8)

    //4
    //16:51  元组 Long: 分区号  Long: key值
    //获取到数据及数据所在分区id
    val mapDataSet: DataSet[(Long, Long)] = filterDataSet.map(new RichMapFunction[Long, (Long, Long)] {
      //重写map方法
      override def map(value: Long): (Long, Long) = {
        //传递一个参数,方法一个参数以及这个参数所在的分区id
        //getRuntimeContext获取运行时的上下文, getRuntimeContext.getIndexOfThisSubtask拿到当前数据的所在分区id
        //MaoFunction无法获取到这个值
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    })
    mapDataSet.print()
  }
}
