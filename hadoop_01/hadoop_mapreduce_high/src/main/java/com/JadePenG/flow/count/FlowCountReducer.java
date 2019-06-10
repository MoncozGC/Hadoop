package com.JadePenG.flow.count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第七步: 自定义reduce逻辑
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {


        /*
        接收数据：
        K2：13480253104
        V2：3	3	180	180 javaBean

        处理数据为：
        K2：13480253104
        V2：3	3	180	180 javaBean
         */
        //1. recData

        //2. op
        Long upFlow = 0L;
        Long downFlow = 0L;
        Long totalUpFlow = 0L;
        Long totalDownFlow = 0L;

        for (FlowBean flowBean : values) {

            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
            totalUpFlow += flowBean.getTotalUpFlow();
            totalDownFlow += flowBean.getTotalDownFlow();


        }
        FlowBean flowBean = new FlowBean(upFlow, downFlow, totalUpFlow, totalDownFlow);

        //3. send
        context.write(key, flowBean);
    }
}















