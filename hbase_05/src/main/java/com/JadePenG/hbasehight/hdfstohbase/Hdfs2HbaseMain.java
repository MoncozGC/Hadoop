package com.JadePenG.hbasehight.hdfstohbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 需求: 读取HDFS文件，写入到HBase表当中去
 * LongWritable：k1
 * Text：v1
 * Text：k2
 * NullWritable：v2
 *
 * @author Peng
 */
public class Hdfs2HbaseMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "Hdfs2HbaseMain");

        //1. 读取文件, 解析成kv键值对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/hbase/input"));

        //2. 自定义Mapper, 接收k1, v1转换成k2, v2输出
        job.setMapperClass(HdfsMapper.class);
        job.setMapOutputKeyClass(TextInputFormat.class);
        job.setMapOutputValueClass(NullWritable.class);

        //3. 分区
        //4. 排序
        //5 规约
        //6. 分组
        //7. reduce逻辑, 接收k2, v2 转换成k3, v3输出
        TableMapReduceUtil.initTableReducerJob(
                //输出的表
                "myUser2",
                //reducer class
                HdfsReducer.class,
                job
        );

        boolean b = job.waitForCompletion(true);

        if (!b) {
            throw new IOException("error with job!");
        }
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        int run = ToolRunner.run(configuration, new Hdfs2HbaseMain(), args);
        System.exit(run);
    }

}
