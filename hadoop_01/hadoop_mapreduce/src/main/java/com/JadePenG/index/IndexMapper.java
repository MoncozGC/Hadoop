package com.JadePenG.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class IndexMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*
        K1：行偏移量
        V1：hello tom

        K2：hello-filename
        V2：1
         */

        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String fileName = inputSplit.getPath().getName();

        String lineData = value.toString();
        String[] wordArr = lineData.split(" ");

        for ( String word : wordArr ){

            context.write(new Text(word+"-"+fileName),new LongWritable(1));

        }



    }
}
