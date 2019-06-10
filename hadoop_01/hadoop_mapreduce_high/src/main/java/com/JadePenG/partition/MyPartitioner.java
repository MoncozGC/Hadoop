package com.JadePenG.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 第三步: 自定义分区
 */
public class MyPartitioner extends Partitioner<LongWritable, Text> {
    @Override
    public int getPartition(LongWritable longWritable, Text text, int numReduceTasks) {

        /*
        接收数据：
        K2：6
        V2：1	0	1	2017-07-31 23:10:12	837255	6	4+1+1=6	小,双	0	0.00	0.00	1	0.00	1	1{1}
        1	0	1	2017-07-31 23:10:12	837255	16	4+1+1=6	小,双	0	0.00	0.00	1	0.00	1	1{0}
        reducetask0
        reducetask1
         */

        //1.接收数据
        long flag = longWritable.get();

        //2.处理数据(打上一个逻辑标识)
        if (flag > 15) {
            return 0;//打上0这个逻辑标识
        } else {
            return 1;//打上1这个逻辑标识
        }

//        return 0;
    }
}














