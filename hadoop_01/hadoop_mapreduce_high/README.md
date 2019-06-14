## 说明

> com.JadePenG.partition

```
需求: 按照数据第6个字段，将原有存在与一个文件中的数据，拆分为2个文件
读取文件: resources\com\JadePenG\partition\input

注意:
    因为是分成两个文件, 
    添加代码	job.setNumReduceTasks(2); 
    所以只能由集群运行

集群运行: 
上传resource--partition下的jar包到服务器中
hadoop jar com.JadePenG.partition.MyPartitionJobMain [输入路径]  [输出路径]

com.JadePenG.pcom.JadePenG.partition	hdfs dfs -mkdir -p [文件夹输入路径]
	hdfs dfs -put partition.csv [文件夹com.JadePenG.partition
```



> com.JadePenG.sort

```
需求: 第一列按照字典顺序进行排列，第一列相同的时候，第二列按照升序进行排列
读取文件: resources\com\JadePenG\sort\input

注意: Mapper发送的V2不能为NullWritable, 否则有相同的数据时会少一条数据. 
比如(传递的V2为NullWritable), 有两个 a,7  一样的数据
因为a,7都是value, 在分区的时候被打上标识{0}, 再记录分组时, 两个a,7 就只有一个a,7 了
```



>com.JadePenG.flow.count

```
需求: 统计每个手机号的上行流量总和，下行流量总和，上行总流量之和，下行总流量之和
```

>com.JadePenG.flow.sort

```
需求: 基于count输出的文件, 以JavaBean的upFlow(上行流量总和)为准, 进行倒序排序

注意: 读取的文件是Count程序对初始文件进行计算过的文件----output_count文件夹中
```

> com.JadePenG.flow.partition

```
需求: 对手机号进行分区

注意: 基于count输出的文件进行分区操作---output_count文件夹中
```

