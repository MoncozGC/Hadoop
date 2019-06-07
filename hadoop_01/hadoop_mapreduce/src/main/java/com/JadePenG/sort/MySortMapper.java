package com.JadePenG.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MySortMapper extends Mapper<LongWritable, Text, SortBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
        接收：
        K1：行偏移量
        V1：a    1

        处理为：
        javabean<a,1>           a    1
         */

        String lineData = value.toString();
        String[] wordArr = lineData.split("\t");

        String first = wordArr[0];
        Integer second = Integer.parseInt(wordArr[1]);

        SortBean sortBean = new SortBean(first, second);

        context.write(sortBean, value);
    }
}
