package com.JadePenG.commonfriend.step01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 7. Reducer逻辑
 * @author Peng
 * @Description
 */
public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        /*
        * 接收
        * K2: B
        * V2: A,E,F,J
        *
        * 处理:
        * K3: A-E-F-J
        * V3: B
        *
        * */

        StringBuffer stringBuffer = new StringBuffer();

        for (Text user : values) {
            stringBuffer.append(user.toString()).append("-");
        }

        context.write(new Text(stringBuffer.toString()), key);
    }
}
