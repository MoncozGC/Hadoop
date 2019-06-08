package com.JadePenG.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第七步: 自定义reduce逻辑
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable countScore = new LongWritable();

    /**
     * 每接收一行数据就执行reduce方法
     *
     * @param key     Mapper发送到Reducer中的key
     * @param values  Mapper发送到Reducer中的value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        /*
         * 数据格式
         *
         * K2:hello
         * V2:{1,1}
         *
         * 处理为
         * K3:hello
         * V3:1+1=2
         */
        //1. 接收数据
        //2. 处理数据
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        countScore.set(count);
        //3.发送到下游

        context.write(key, countScore);
    }
}















