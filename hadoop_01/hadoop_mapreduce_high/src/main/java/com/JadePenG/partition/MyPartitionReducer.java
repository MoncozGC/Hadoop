package com.JadePenG.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第七步: 自定义reduce逻辑
 */
public class MyPartitionReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        /*
        接收数据：
        K2：6
        V2：1	0	1	2017-07-31 23:10:12	837255	6	4+1+1=6	小,双	0	0.00	0.00	1	0.00	1	1

        处理数据为：
        K3：1	0	1	2017-07-31 23:10:12	837255	6	4+1+1=6	小,双	0	0.00	0.00	1	0.00	1	1
        V3：Null
         */
        for (Text value : values) {
            context.write(value, NullWritable.get());
        }


    }
}















