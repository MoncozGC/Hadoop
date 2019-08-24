package com.JadePenG.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 需求：
 * 有大量的文本（文档、网页），需要建立搜索索引
 * 作用：
 * 1.整合mapreduce程序3阶段8步骤
 * <p>
 * 2.mapreduce程序的入口
 */
public class IndexJobMain extends Configured implements Tool {


    /*
    1.整合mapreduce程序3阶段8步骤
     */
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "index");
        job.setJarByClass(IndexJobMain.class);//如果不写这行代码，在集群模式下运行会报错

        /*
        1.map阶段
         */
        //1.指定输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,
                new Path("H:\\~Big Data\\Employment\\03_大数据阶段\\Hadoop阶段\\day05_MapReduce高阶训练及Yarn资源调度\\资料\\倒排索引\\input"));


        //2.自定义map逻辑
        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        /*
        2.shuffle阶段
         */
        //3.分区
        //4.排序
        //5.规约
        //6.分组

        /*
        3.reduce阶段
         */
        //7.自定义reduce逻辑
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //8.指定输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,
                new Path("H:\\~Big Data\\Employment\\03_大数据阶段\\Hadoop阶段\\day05_MapReduce高阶训练及Yarn资源调度\\资料\\倒排索引\\output"));

        //等到执行
        job.waitForCompletion(true);

        return 0;
    }

    /*
    2.mapreduce程序的入口
     */
    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Configuration(), new IndexJobMain(), args);

    }

}
