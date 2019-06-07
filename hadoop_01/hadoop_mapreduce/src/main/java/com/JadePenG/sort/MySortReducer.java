package com.JadePenG.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MySortReducer extends Reducer<SortBean, Text, SortBean, Text> {

    @Override
    protected void reduce(SortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /*
         * 接收数据
         * K2: javaBean<a,1>
         * V2: a 1
         *
         * 处理数据
         * K3: javaBean<a,1>
         * V3: a 1
         * */

        for (Text value : values) {
            context.write(key, value);
        }

    }
}
