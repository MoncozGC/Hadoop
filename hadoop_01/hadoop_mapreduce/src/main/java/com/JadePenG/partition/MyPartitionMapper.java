package com.JadePenG.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 第二步: 自定义mapper逻辑
 */
public class MyPartitionMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    /*
    来一行数据，就执行一次下面的map方法
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*
        接收数据：
        K1：行偏移量
        V1：1	0	1	2017-07-31 23:10:12	837255	6	4+1+1=6	小,双	0	0.00	0.00	1	0.00	1	1

        处理为：
        K2：6
        V2：1	0	1	2017-07-31 23:10:12	837255	6	4+1+1=6	小,双	0	0.00	0.00	1	0.00	1	1
         */

        //1.接收数据
        String lineData = value.toString();
        String[] fieldArr = lineData.split("\t");

        //2.处理数据
        Long fieldSix = Long.parseLong(fieldArr[5]);


        //3.发送到下游
        context.write(new LongWritable(fieldSix), value);

    }
}