package com.JadePenG.hbasehight.hdfstohbase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * LongWritable：k1
 * Text：v1
 * Text：k2
 * NullWritable：v2
 * @author Peng
 */
public class HdfsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    /**
     * 在map阶段不做任何处理，直接将读取到的数据返回
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}
