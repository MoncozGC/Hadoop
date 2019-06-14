package com.JadePenG.flow.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;

/**
 *
 * 对手机号进行分区
 * @author Peng
 * @Description
 */
public class MyPartitioner extends Partitioner<Text, NullWritable> {


    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {

        /*
        K2:13480253104	3	3	180	180
        V2:NullWritable
         */

        String LineData = text.toString();
        String[] fieldArr = LineData.split("\t");
        String phoneNum = fieldArr[0];

        if (phoneNum.startsWith("135")) {
            return 0;
        } else if (phoneNum.startsWith("136")) {
            return 1;
        } else if (phoneNum.startsWith(("137"))) {
            return 2;
        } else if (phoneNum.startsWith("138")) {
            return 3;
        } else if (phoneNum.startsWith(("139"))) {
            return 4;
        } else {
            return 5;
        }
    }
}
