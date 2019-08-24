package com.JadePenG.flow.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*
        rec:
        K1:offset
        V1:13480253104	3	3	180	180

        op:
        K2:13480253104	3	3	180	180
        V2: null
         */

        context.write(value, NullWritable.get());
    }
}
