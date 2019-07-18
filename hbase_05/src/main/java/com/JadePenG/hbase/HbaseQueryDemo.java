package com.JadePenG.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.AfterTest;

import java.io.IOException;


/**
 * 查询操作
 * Hbase有三种查询数据的方式
 * 1. 通过行键rowKey直接查询, 效率最高
 * 2. 指定起始rowKey和结束rowKey进行扫描
 * 3. 全表扫描[很少用]
 *
 * @author Peng
 */
public class HbaseQueryDemo {
    //创建Hbase的Client连接
    private Connection connection;
    private Table table;

    /**
     * 创建连接获取属性
     *
     * @throws IOException
     */
    @Before
    public void initTableConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //指定zk的连接地址，zk里面保存了hbase的元数据信息
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        //连接Hbase的服务器
        connection = ConnectionFactory.createConnection(configuration);
        //指定查询的表
        TableName tableName = TableName.valueOf("myUser");
        //获取table的表对象
        table = connection.getTable(tableName);
    }

    /**
     * 释放资源
     *
     * @throws IOException
     */
    @AfterTest
    public void close() throws IOException {
        table.close();
        connection.close();
    }

    /**
     * 根据rowKey进行查询数据
     * get 'myUser','0001'
     * get 'myUser','0001','f1'
     * get 'myUser','0001','f1:name' (缩小了查询范围，类似于mysql： select name from myuser)
     * <p>
     * id和age是int类型所以需要Bytes.toInt输出
     *
     * @throws IOException
     */
    @Test
    public void getByRowKey() throws IOException {
        //通过rowKey查询数据
        //获取table
        Get get = new Get("0001".getBytes());
        //指定数据从哪个列族中查找
        get.addFamily("f1".getBytes());
        //get.addFamily("f2".getBytes());
        get.addColumn("f1".getBytes(), "name".getBytes());
        //通过table.get方式传递get进行查询
        Result result = table.get(get);
        //工具类输出
        HbasePrintlnUtil.printResult(result);

        //使用工具类代替
        //获取我们所有列的值，所有列的值都封装到了cell数组里面
//        Cell[] cells = result.rawCells();
//        for (Cell cell : cells){
//            //获取每一个cell的值的字节数组(只打印了三个列值，实际上存储的是5个)
//            byte[] value = cell.getValue();
//            //因为id和age是int类型所以需要Bytes.toInt输出
//            //获取指定列的列名
//            byte[] qualifier =  CellUtil.cloneQualifier(cell);
//            if(Bytes.toString(qualifier).equals("id") || Bytes.toString(qualifier).equals("age")){
//                System.out.println(Bytes.toInt(value));
//            }else {
//                System.out.println(Bytes.toString(value));
//            }
//            //System.out.println(Bytes.toInt(value));
//        }
//        byte[] rows =  result.getRow();
//        System.out.println(Bytes.toString(rows));
    }


    @Test
    public void scanRange() throws IOException {
        //通过Scan对象, 来获取我们的起始范围和结束范围
        Scan scan = new Scan();
        //通过指定起始rowKey和endRowKey实现范围的扫描, 包左不包右

        scan.setStopRow("0006".getBytes());
        //通过getScanner返回值的类型是ResultScanner
        ResultScanner scanner = table.getScanner(scan);
        HbasePrintlnUtil.printResultScanner(scanner);

        //使用工具类代替
//        for (Result result:scanner){
//            //获取每一个cell的值的字节数组(只打印了三个列值，实际上存储的是5个)
//            byte[] row = result.getRow();
//            System.out.println(Bytes.toString(row));
//            Cell[] cells = result.rawCells();
//            for (Cell cell : cells){
//                //获取每一个cell的值的字节数组(只打印了三个列值，实际上存储的是5个)
//                byte[] value = cell.getValue();
//                //因为id和age是int类型所以需要Bytes.toInt输出
//                //获取指定列的列名
//                byte[] qualifier =  CellUtil.cloneQualifier(cell);
//                if(Bytes.toString(qualifier).equals("id") || Bytes.toString(qualifier).equals("age")){
//                    System.out.println(Bytes.toInt(value));
//                }else {
//                    System.out.println(Bytes.toString(value));
//                }
//                //System.out.println(Bytes.toInt(value));
//            }
//        }
    }

    /**
     * 通过startRowKey和endRowKey进行扫描查询
     */
    @Test
    public void scanRowKey() throws IOException {
        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table myUser = connection.getTable(TableName.valueOf("myUser"));
        Scan scan = new Scan();
        scan.setStartRow("0004".getBytes());
        scan.setStopRow("0006".getBytes());
        ResultScanner resultScanner = myUser.getScanner(scan);
        for (Result result : resultScanner) {
            //获取rowKey
            System.out.println(Bytes.toString(result.getRow()));
            //遍历获取得到所有的列族以及所有的列的名称
            KeyValue[] raw = result.raw();
            for (KeyValue keyValue : raw) {
                //获取所属列族
                System.out.println(Bytes.toString(keyValue.getFamilyArray(), keyValue.getFamilyOffset(), keyValue.getFamilyLength()));
                System.out.println(Bytes.toString(keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength()));
            }
            //指定列族以及列打印列当中的数据出来
            System.out.println(Bytes.toInt(result.getValue("f1".getBytes(), "id".getBytes())));
            System.out.println(Bytes.toInt(result.getValue("f1".getBytes(), "age".getBytes())));
            System.out.println(Bytes.toString(result.getValue("f1".getBytes(), "name".getBytes())));
        }
        myUser.close();
    }
}
