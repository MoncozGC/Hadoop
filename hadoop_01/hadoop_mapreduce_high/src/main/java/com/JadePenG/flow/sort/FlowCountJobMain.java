package com.JadePenG.flow.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 需求: 统计每个手机号的上行流量总和，下行流量总和，上行总流量之和，下行总流量之和
 * 1. 整合mapReduce程序的3阶段8步骤
 * 2. 主程序入口
 *
 * 读取文件基于Count程序对初始文件进行计算过的文件----output_count文件夹中
 */
public class FlowCountJobMain extends Configured implements Tool {
    /*
    1.整合mapReduce程序的3阶段8步骤
     */
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "count");
        job.setJarByClass(FlowCountJobMain.class);

        /*
        1.map阶段
         */
        //1.指定输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,
                new Path("hadoop_01\\hadoop_mapreduce_high\\src\\main\\resources\\com\\JadePenG\\flow\\count\\output_count"));

        //2.自定义map逻辑
        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

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
        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //8.指定输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hadoop_01\\hadoop_mapreduce_high\\src\\main\\resources\\com\\JadePenG\\flow\\count\\output_sort"));

        //等待完成
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    /*
    2.作为我们mapReduce程序的入口
     */
    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new Configuration(), new FlowCountJobMain(), args);
        System.exit(run);

    }

}

