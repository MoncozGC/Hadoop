package com.JadePenG.flow.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*
        rec:
        K1:offset
        V1:1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com	游戏娱乐	24	27	2481	24681	200

        op:
        K2:13726230503
        V2:flowBean
         */

        //1.recData
        String lineData = value.toString();

        //2.op
        String[] fieldArr = lineData.split("\t");
        String phoneNum = fieldArr[1];

        Long upFlow = Long.parseLong(fieldArr[6]);
        Long downFlow = Long.parseLong(fieldArr[7]);
        Long totalUpFlow = Long.parseLong(fieldArr[8]);
        Long totalDownFlow = Long.parseLong(fieldArr[9]);

        FlowBean flowBean = new FlowBean(upFlow, downFlow, totalUpFlow, totalDownFlow);

        //3.send
        context.write(new Text(phoneNum), flowBean);


    }
}
