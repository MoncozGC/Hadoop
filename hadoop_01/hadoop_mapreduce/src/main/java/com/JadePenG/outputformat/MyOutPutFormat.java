package com.JadePenG.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutPutFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path enhancePath = new Path("H:\\~Big Data\\Employment\\03_大数据阶段\\Hadoop阶段\\day05_MapReduce高阶训练及Yarn资源调度\\资料\\自定义outputformat\\badcomment\\badComment.txt");
        Path toCrawlPath = new Path("H:\\~Big Data\\Employment\\03_大数据阶段\\Hadoop阶段\\day05_MapReduce高阶训练及Yarn资源调度\\资料\\自定义outputformat\\goodcomment\\goodComment.txt");
        FSDataOutputStream enhanceOut = fs.create(enhancePath);
        FSDataOutputStream toCrawlOut = fs.create(toCrawlPath);
        return new MyRecordWriter(enhanceOut,toCrawlOut);
    }

    static class MyRecordWriter extends RecordWriter<Text, NullWritable>{

        FSDataOutputStream enhanceOut = null;
        FSDataOutputStream toCrawlOut = null;

        public MyRecordWriter(FSDataOutputStream enhanceOut, FSDataOutputStream toCrawlOut) {
            this.enhanceOut = enhanceOut;
            this.toCrawlOut = toCrawlOut;
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            if (key.toString().split("\t")[9].equals("0")){
                toCrawlOut.write(key.toString().getBytes());
                toCrawlOut.write("\r\n".getBytes());
            }else{
                enhanceOut.write(key.toString().getBytes());
                enhanceOut.write("\r\n".getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if(toCrawlOut!=null){
                toCrawlOut.close();
            }
            if(enhanceOut!=null){
                enhanceOut.close();
            }
        }
    }
}

