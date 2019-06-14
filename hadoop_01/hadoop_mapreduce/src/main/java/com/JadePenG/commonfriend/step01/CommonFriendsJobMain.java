package com.JadePenG.commonfriend.step01;

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
 * 需求: 求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？
 *
 * @author Peng
 * @Description
 */
public class CommonFriendsJobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "commonFriend");
        job.setJarByClass(CommonFriendsJobMain.class);

        //指定输入类型及路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,
                new Path("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_MapReduce高阶训练及Yarn资源调度\\资源\\共同好友\\input"));

        //2. Mapper自定义逻辑
        job.setMapperClass(CommonFriendsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        //7. Reducer自定义逻辑
        job.setReducerClass(CommonFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定输出类型及路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_MapReduce高阶训练及Yarn资源调度\\资源\\共同好友\\output_step1"));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new CommonFriendsJobMain(), args);
    }
}
