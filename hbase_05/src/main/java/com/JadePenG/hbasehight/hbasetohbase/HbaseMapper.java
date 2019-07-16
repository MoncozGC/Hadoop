package com.JadePenG.hbasehight.hbasetohbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

/**
 * @author Peng
 */
public class HbaseMapper extends TableMapper<Text, Put> {

    /**
     * @param key     rowKey
     * @param value   一行数据所有列的值都封装在value里面了
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取到key, key就是rowKey
        byte[] bytes = key.get();
        //创建一个put对象, v2返回的就是它
        Put put = new Put(bytes);
        List<Cell> cells = value.listCells();
        for (Cell cell : cells) {
            //解析name和age字段
            //判断属于哪个列族
            byte[] family = CellUtil.cloneFamily(cell);
            //获取cell属于那一列
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            if (Bytes.toString(family).equals("f1")) {
                if (Bytes.toString(qualifier).equals("name") || Bytes.toString(qualifier).equals("age")) {
                    put.add(cell);
                }
            }
        }
        if (!put.isEmpty()) {
            //将myUser读到的每行数据写入到context作为map输出
            context.write(new Text(Bytes.toString(bytes)), put);
        }
    }
}
