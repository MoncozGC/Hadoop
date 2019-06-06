## 说明

MRAppMaster：负责整个程序的过程调度及状态协调
MapTask：负责map阶段的整个数据处理流程
ReduceTask：负责reduce阶段的整个数据处理流程

## 具体步骤:
 * Map阶段2个步骤


> 第一步：设置inputFormat类，将我们的数据切分成key，value对，输入到第二步
> 第二步：自定义map逻辑，处理我们第一步的输入数据，然后转换成新的key，value对进行输出

 * shuffle阶段4个步骤（可以全部不用管）

> 第三步：对输出的key，value对进行分区
> 第四步：对不同分区的数据按照相同的key进行排序
> 第五步：对分组后的数据进行规约(combine操作)，降低数据的网络拷贝（可选步骤）
> 第六步：对排序后的额数据进行分组，分组的过程中，将相同key的value放到一个集合当中

* reduce阶段2个步骤

> 第七步：对多个map的任务进行合并，排序，写reduce函数自己的逻辑，对输入的key，value对进行处理，转换成新的key，value对进行输出
> 第八步：设置outputFormat将输出的key，value对数据进行保存到文件中

## mapReduce编程模型的总结:

MapReduce的开发一共有八个步骤其中

map阶段分为2个步骤

shuffle阶段4个步骤

reduce阶段分为2个步骤



### Jar包的运行方式:

```properties
# 在虚拟机hdfs中创建目录 
hdfs dfs -mkdir -p /myText/wordCount_day03/input 

# 并且将wordcount.txt上传到input目录下
hdfs dfs -put /myText/wordCount_day03/input 

# 运行jar包 复制主函数入口
hadoop jar wordCount.jar com.JadePenG.mr.WordCountJobMain

# 会有一个InterruptedException异常报错, 属于正常情况

```

