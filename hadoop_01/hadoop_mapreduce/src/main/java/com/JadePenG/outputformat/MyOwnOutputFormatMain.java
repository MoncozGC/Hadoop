package com.JadePenG.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MyOwnOutputFormatMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, MyOwnOutputFormatMain.class.getSimpleName());
        job.setJarByClass(MyOwnOutputFormatMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("H:\\~Big Data\\Employment\\03_大数据阶段\\Hadoop阶段\\day05_MapReduce高阶训练及Yarn资源调度\\资料\\自定义outputformat\\input"));
        job.setMapperClass(MyOwnMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(MyOutPutFormat.class);
        //设置一个输出目录，这个目录会输出一个success的成功标志的文件
        MyOutPutFormat.setOutputPath(job,new Path("H:\\~Big Data\\Employment\\03_大数据阶段\\Hadoop阶段\\day05_MapReduce高阶训练及Yarn资源调度\\资料\\自定义outputformat\\output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static class MyOwnMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] split = value.toString().split("\t");
//            String commentStatus = split[9];
            context.write(value,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        ToolRunner.run(configuration,new MyOwnOutputFormatMain(),args);
    }
}

