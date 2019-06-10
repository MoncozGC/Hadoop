package com.JadePenG.commonfriend.step02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 7. Reducer逻辑
 *
 * @author Peng
 * @Description
 */
public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        /*
        * 接收
        * K2:   A-E
                E-J
                E-F
                A-J
                A-F
                F-J
        * V2: {B,C}
        *
        * 处理:
        * K3: A-E
        * V3: B-C
        *
        * */
//        System.out.println("Text key\t"+key);
//        System.out.println("Iterable<Text> values\t"+values);

        StringBuffer stringBuffer = new StringBuffer();
        for (Text friend : values) {
            stringBuffer.append(friend).append("-");
//            System.out.println("Text friend\t"+friend);
        }

        context.write(key, new Text(stringBuffer.toString()));
    }
}
