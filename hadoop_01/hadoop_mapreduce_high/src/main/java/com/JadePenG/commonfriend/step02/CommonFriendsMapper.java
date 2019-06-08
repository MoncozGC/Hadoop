package com.JadePenG.commonfriend.step02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * 2. Mapper逻辑
 *
 * @author Peng
 * @Description Mapper<LongWritable                                                               ,                                                                                                                               Text                                                               ,                                                                                                                               Text                                                               ,                                                                                                                               Text>  前两个泛型: 接收数据的类型    后两个泛型: 发送数据的类型
 */
public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
        * 接收数据:
        * K1: 行偏移量
        * V1: E-A-J-F-	B
        *
        * 处理成:
        * K2:   E-A
                E-J
                E-F
                A-J
                A-F
                J-F
        * V2: B
        *
        * */

        //1 接收数据
        String lineData = value.toString();
        String[] split = lineData.split("\t");

        //E-A-J-F-
        String userList = split[0];
        //B
        String commonFriend = split[1];

        //2. 处理数据
        String[] userArr = userList.split("-");
        //进行正序排序  {A,E,F,J}
        Arrays.sort(userArr);

        //外层for循环控制的是“-”扛号前的用户
        for (int i = 0; i < userArr.length - 1; i++) {
//            System.out.println("i\t" + i + "\t" + userArr[i]);
            //内层for循环控制的是“-”扛号后的用户
            for (int j = i + 1; j < userArr.length; j++) {
//                System.out.println("j\t" + j + "\t" + userArr[j]);
                context.write(new Text(userArr[i] + "-" + userArr[j]), new Text(commonFriend));
            }
        }
    }
}
