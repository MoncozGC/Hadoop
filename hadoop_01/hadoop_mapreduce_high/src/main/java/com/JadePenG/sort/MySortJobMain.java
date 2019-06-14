package com.JadePenG.sort;

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
 * 需求: 第一列按照字典顺序进行排列，第一列相同的时候，第二列按照升序进行排列
 */
public class MySortJobMain extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(super.getConf(), "mySort");
        job.setJarByClass(MySortJobMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,
                new Path("hadoop_01\\hadoop_mapreduce_high\\src\\main\\resources\\com\\JadePenG\\sort\\input"));

        //2. 自定义mapper逻辑
        job.setMapperClass(MySortMapper.class);
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(Text.class);

        //7. 自定义reduce逻辑
        job.setReducerClass(MySortReducer.class);
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(Text.class);

        //8. 指定输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,
                new Path("hadoop_01\\hadoop_mapreduce_high\\src\\main\\resources\\com\\JadePenG\\sort\\output"));

        //等待完成
        boolean b = job.waitForCompletion(true);


        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new Configuration(), new MySortJobMain(), args);
        System.exit(run);

    }
}
