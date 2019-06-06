package com.JadePenG.mr;

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
 * 需求: 统计词频
 * 1. 整合mapReduces三个阶段八个步骤
 * 2. 作为入口程序
 */
public class WordCountJobMain extends Configured implements Tool {
    /**
     * 1. 整合mapReduces三个阶段八个步骤
     */
    @Override
    public int run(String[] strings) throws Exception {

        //整合的媒介
        Job job = Job.getInstance(super.getConf(), "wordCount");

        //如果不写这行代码, 在集群模式下会报错
        job.setJarByClass(WordCountJobMain.class);

        //指定待处理的文件格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,
                new Path("/myText/wordCount_day03/input"));
        /*
         * 1. map阶段
         *   指定输入
         *
         *   自定义map逻辑
         */
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        /* 2. shuffle阶段
         *
         *   分区
         *   排序
         *   规约
         *   分组
         */
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        /* 3. reduce阶段
         *   自定义reduce逻辑
         *   指定输出
         *
         * */
        job.setOutputFormatClass(TextOutputFormat.class);
        //指定输出的路径path, 一定不能已经存在, 否则会报错
        TextOutputFormat.setOutputPath(job,
                new Path("/myText/wordCount_day03/Output"));

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    /**
     * 2. 作为入口程序
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new WordCountJobMain(), args);
        System.exit(run);
    }
}
