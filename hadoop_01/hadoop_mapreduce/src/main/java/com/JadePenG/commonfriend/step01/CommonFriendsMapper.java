package com.JadePenG.commonfriend.step01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 2. Mapper逻辑
 * @author Peng
 * @Description
 *
 * Mapper<LongWritable, Text, Text, Text>  前两个泛型: 接收数据的类型    后两个泛型: 发送数据的类型
 */
public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
        * 接收数据:
        * K1: 行偏移量
        * V1: 一行数据  A:B,C,D,F,E,O
        *
        * 处理成:
        * K2: B
        * V2: A,E,F,J
        *
        * */

        String lineData = value.toString();
        String[] split = lineData.split(":");

        String strUser = split[0];
        String friendsList = split[1];
        String[] friends = friendsList.split(",");

        for (String friend : friends) {
            context.write(new Text(friend), new Text(strUser));
        }
    }
}
