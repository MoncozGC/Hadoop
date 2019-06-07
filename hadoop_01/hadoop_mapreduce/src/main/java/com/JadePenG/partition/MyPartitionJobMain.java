package com.JadePenG.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 需求: 按照数据第6个字段，将原有存在与一个文件中的数据，拆分为2个文件
 * 1. 整合mapReduce程序的3阶段8步骤
 * 2. 主程序入口
 */
public class MyPartitionJobMain extends Configured implements Tool {
    /*
    1.整合mapreduce程序的3阶段8步骤
     */
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "myPartition");
        //如果不写这行代码，集群模式下运行会报错
        job.setJarByClass(MyPartitionJobMain.class);

        /*
        1.map阶段
         */
        //1.指定输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        //2.自定义map逻辑
        job.setMapperClass(MyPartitionMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        /*
        2.shuffle阶段
         */
        //3.分区
        job.setPartitionerClass(MyPartitioner.class);
        //4.排序
        //5.规约
        //6.分组

        /*
        3.reduce阶段
         */
        //7.自定义reduce逻辑
        job.setReducerClass(MyPartitionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        /*
        1.设置reducetask的数量，reducetask的数量与输出文件的个数是一一对应的
        2.如果我们写代码设置了reducetask数量，而不是没有写这行代码，走默认的一个reducetask数量的话，
        就必须提交到集群上运行
         */
        job.setNumReduceTasks(2);

        //8.指定输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        //等待完成
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    /*
    2.作为我们mapreduce程序的入口
     */
    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new Configuration(), new MyPartitionJobMain(), args);
        System.exit(run);

    }

}

