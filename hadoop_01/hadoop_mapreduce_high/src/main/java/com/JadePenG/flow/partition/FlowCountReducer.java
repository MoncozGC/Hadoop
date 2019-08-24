package com.JadePenG.flow.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第七步: 自定义reduce逻辑
 */
public class FlowCountReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {


        /*
        接收数据：
        K2：13480253104
        V2：3	3	180	180 javaBean

        处理数据为：
        K2：13480253104
        V2：3	3	180	180 javaBean
         */
        context.write(key, NullWritable.get());

    }
}















