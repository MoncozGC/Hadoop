package com.JadePenG.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private String first;
    private Integer second;

    public SortBean() {
    }

    public SortBean(String first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return first + "\t" + second ;
    }


    //比较的方法
    @Override
    public int compareTo(SortBean o) {
        //先比较第一列first, 如果第一列相等 再比较第二列second

        /*
        * compareTo的作用
        *   前面等于后面，返回0
            前面大于后面，返回正值
            前面小于后面，返回负值

            a == b ? 0 : (!a ? -1 : 1);
        * */
        int compare = this.first.compareTo(o.getFirst());
        if (0 == compare) {
            compare = this.second.compareTo(o.getSecond());
        }

        return compare;
    }

    //序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readUTF();
        second = dataInput.readInt();
    }
}
