package com.JadePenG.hbasehight.hdfstohbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author Peng
 */
public class HdfsReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {

    /**
     * 0007    zhangsan        18
     * 0008    liSi    25
     * 0009    wangWu  20
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //将数据切割
        String[] split = key.toString().split("\t");
        //获取rowKey
        Put put = new Put(split[0].getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), split[1].getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), split[2].getBytes());

        context.write(new ImmutableBytesWritable(split[0].getBytes()), put);
    }
}
