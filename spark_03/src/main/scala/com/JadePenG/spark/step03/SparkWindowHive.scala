package com.JadePenG.spark.step03

import org.apache.spark.sql.SparkSession

/**
  *开窗函数、行列转换
  * 有json数据格式如下，分别是三个字段，对应学生姓名，所属班级，所得分数，求每个班级当中分数最高的前N个学生（分组求topN）
  * 创建hive表，加载score.txt文件
  * @author Peng
  */
object SparkWindowHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").enableHiveSupport().getOrCreate()

    val jsonDF = spark.read.json("H:\\~Big Data\\Employment\\03_大数据阶段\\day06_Spark Sql\\资料\\score.txt")

    jsonDF.createOrReplaceTempView("v_score")


    /**
      * over（）开窗函数：
      * 在使用聚合函数后，会将多行变成一行，而开窗函数是将一行变成多行
      * 并且在使用聚合函数后，如果要显示其他的列必须将列加入到group by中
      * 而使用开窗函数后，可以不使用group by，直接将所有信息显示出来。
      *  开窗函数适用于在每一行的最后一列添加聚合函数的结果
      *
      *   1.为每条数据显示聚合信息.(聚合函数() over())
      *   2.为每条数据提供分组的聚合函数结果(聚合函数() over(partition by 字段) as 别名)  按照字段分组，分组后进行计算
      *   3.与排名函数一起使用(row number() over(order by 字段) as 别名) 常用分析函数：（最常用的应该是1.2.3 的排序）
      *
      * 常用的开窗函数：
      * 1、row_number() over(partition by ... order by ...)\n" +   这个函数不需要考虑是否并列，哪怕根据条件查询出来的数值相同也会进行连续排名
      * 2、rank() over(partition by ... order by ...)\n" +         使用这个函数，成绩相同的两名并列，下一位同学空出所占的名次
      * 3、dense_rank() over(partition by ... order by ...)\n" +   使用这个函数，成绩相同的两名并列，下一位同学不空所占名次
      * 4、count() over(partition by ... order by ...)\n" +
      * 5、max() over(partition by ... order by ...)\n" +
      * 6、min() over(partition by ... order by ...)\n" +
      * 7、sum() over(partition by ... order by ...)\n" +
      * 8、avg() over(partition by ... order by ...)\n" +
      * 9、first_value() over(partition by ... order by ...)\n" +
      * 10、last_value() over(partition by ... order by ...)\n" +
      * 11、lag() over(partition by ... order by ...)\n" +
      * 12、lead() over(partition by ... order by ...)\n" +
      */

    spark.sql("select * from (select name,clazz,score,row_number() over(partition by clazz order by score desc) rank_over from v_score) temp_scores where rank_over<=3").show()

    spark.stop()


  }
}
