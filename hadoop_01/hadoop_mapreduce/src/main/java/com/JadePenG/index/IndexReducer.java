package com.JadePenG.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        /*
        K2：hello-filename
        V2：{1,1,1}

        k3:hello-filename
        V3:1+1+1=3
         */
        long nCount = 0L;
        for ( LongWritable value : values ){

            nCount += value.get();

        }

        context.write(key,new LongWritable(nCount));

    }
}
