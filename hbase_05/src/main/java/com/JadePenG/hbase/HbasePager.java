package com.JadePenG.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * 分页操作优化
 * startRow起始  endRow(下一页的起始页)
 * 001      startRow="" endRow=003   001002
 * 002
 * 003      startRow=003 endRow=005  003004
 * 004
 * 005      startRow=005 endRow=007  005006
 * 006
 *
 * @author Peng
 */
public class HbasePager {

    public static void main(String[] args) throws IOException {
        ResultScanner resultScanner = GetPageData(4, 2);
        HbasePrintlnUtil.printResultScanner(resultScanner);
    }

    public static Connection initTableConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //指定zk的连接地址，zk里面保存了hbase的元数据信息
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        //连接Hbase的服务器
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    /**
     * 传递页码和每页数量返回pageScanner
     *
     * @param pageIndex 页码
     * @param pageSize  每页显示条数
     * @return
     */
    public static ResultScanner GetPageData(int pageIndex, int pageSize) throws IOException {
        //1. 找到起始rowKey
        //2. 根据起始rowKey返回每页的数据
        String startRow = GetCurrentPageStartRow(pageIndex, pageSize);
        return GetPageData(startRow, pageSize);
    }

    /**
     * 获取起始rowKey
     *
     * @param pageIndex
     * @param pageSize
     * @return
     */
    private static String GetCurrentPageStartRow(int pageIndex, int pageSize) throws IOException {
        //如果传过来的pageIndex是1或者小于1 返回null
        if (pageIndex <= 1) {
            return null;
        } else {
            //从第二页开始
            String startRow = null;
            for (int i = 1; i < pageIndex; i++) {
                //第几次循环就是获取第几页的数据
                ResultScanner pageData = GetPageData(startRow, pageSize);
                //获取当前页的最后一个rowKey
                Iterator<Result> iterator = pageData.iterator();
                Result result = null;
                while (iterator.hasNext()) {
                    //返回下一条数据
                    result = iterator.next();
                }
                //拿到最后一个rowKey
                String endRowStr = new String(result.getRow());
                //在最后一个rowKey上追加一个空字符串 0001->0001
                byte[] add = Bytes.add(endRowStr.getBytes(), new byte[]{0x00});
                String nextPageStartRowStr = Bytes.toString(add);
                startRow = nextPageStartRowStr;
            }
            return startRow;
        }
    }

    /**
     * 从starTrow开始查询pageSize条数据
     *
     * @param startRow 传递页码
     * @param pageSize 每页条数
     * @return
     */
    public static ResultScanner GetPageData(String startRow, int pageSize) throws IOException {
        Connection connection = initTableConnection();
        //指定具体的表
        TableName tableName = TableName.valueOf("myUser");
        Table table = connection.getTable(tableName);

        Scan scan = new Scan();

        //设置起始rowKey
        if (!StringUtils.isBlank(startRow)) {
            scan.setStartRow(startRow.getBytes());
        }
        //设置过滤条件
        PageFilter pageFilter = new PageFilter(pageSize);
        scan.setFilter(pageFilter);
        ResultScanner scanner = table.getScanner(scan);
        return scanner;

    }
}