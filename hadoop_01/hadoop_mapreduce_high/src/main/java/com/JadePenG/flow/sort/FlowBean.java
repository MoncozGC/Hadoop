package com.JadePenG.flow.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Peng
 * @Description
 */
public class FlowBean implements WritableComparable<FlowBean> {

    //上行流量总和，下行流量总和，上行总流量之和，下行总流量之和
    private Long upFlow;
    private Long downFlow;
    private Long totalUpFlow;
    private Long totalDownFlow;

    public FlowBean() {
    }

    public FlowBean(Long upFlow, Long downFlow, Long totalUpFlow, Long totalDownFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalUpFlow = totalUpFlow;
        this.totalDownFlow = totalDownFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getTotalUpFlow() {
        return totalUpFlow;
    }

    public void setTotalUpFlow(Long totalUpFlow) {
        this.totalUpFlow = totalUpFlow;
    }

    public Long getTotalDownFlow() {
        return totalDownFlow;
    }

    public void setTotalDownFlow(Long totalDownFlow) {
        this.totalDownFlow = totalDownFlow;
    }

    @Override
    public String toString() {
        return upFlow +
                "\t" + downFlow +
                "\t" + totalUpFlow +
                "\t" + totalDownFlow;
    }


    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(totalUpFlow);
        dataOutput.writeLong(totalDownFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        totalUpFlow = dataInput.readLong();
        totalDownFlow = dataInput.readLong();
    }

    //比较的方法
    @Override
    public int compareTo(FlowBean o) {

        int compare = this.upFlow.compareTo(o.getUpFlow());

        //倒序
        return -compare;
    }
}
