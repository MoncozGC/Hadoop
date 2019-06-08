package com.JadePenG.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 第二步: 自定义map逻辑
 * <p>
 * extends Mapper<Long, String, String, Long> 不可以这样写泛型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text wordText = new Text();
    private LongWritable score = new LongWritable(1);

    /**
     * 每接收一行数据就执行一次map方法
     *
     * @param key     磁盘中的key
     * @param value   磁盘中的value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*接收数据
         *
         * K1: 行偏移量
         * V1: hello,world,hadoop
         *
         * 处理为:
         * K2: hello
         * V2: 1
         */

        //1. 接收数据
        String lineData = value.toString();
        String[] wordAdd = lineData.split(",");

        //2. 处理数据
        for (String word : wordAdd) {
            wordText.set(word);
            //3. 发送到下游
            context.write(wordText, score);
        }


    }
}












