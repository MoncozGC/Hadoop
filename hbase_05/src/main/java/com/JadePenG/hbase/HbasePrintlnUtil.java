package com.JadePenG.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * 数据查询结果的输出
 *
 * @author Peng
 */
public class HbasePrintlnUtil {
    /**
     * 传递resultScanner对象进行输出
     *
     * @param resultScanner
     */
    public static void printResultScanner(ResultScanner resultScanner) {
        for (Result result : resultScanner) {
            printResult(result);
        }
    }

    /**
     * 传递result对象进行输出
     *
     * @param result
     */
    public static void printResult(Result result) {
        //获取我们所有列的值，所有列的值都封装到了cell数组里面
        List<Cell> cells = result.listCells();
        for (int i = 0; i < cells.size(); i++) {
            Cell cell = cells.get(i);
            printCell(cell);
        }
    }

    /**
     * 打印列数据
     *
     * @param cell
     */
    public static void printCell(Cell cell) {
        System.out.println(
                //一行数据 列族 列名 时间戳
                Bytes.toString(cell.getRow()) + "\t" +
                        Bytes.toString(cell.getFamily()) + "\t" +
                        Bytes.toString(cell.getQualifier()) + "\t" +
                        Bytes.toString(cell.getValue()) + "\t" +
                        cell.getTimestamp()
        );
    }
}
