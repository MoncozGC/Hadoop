package com.JadePenG.hbasehight.hbasetohbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Text: key3的类型
 * Put：value3的类型
 * ImmutableBytesWritable：k4的类型
 *
 * @author Peng
 */
public class HbaseReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {

    /**
     *
     * @param key     就是key3
     * @param values  就是value3
     * @param context 将我们的数据向外写出（上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put put : values) {
            context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
        }
    }

}
