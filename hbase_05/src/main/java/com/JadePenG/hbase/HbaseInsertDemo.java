package com.JadePenG.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase的基本操作，增删改查操作
 *
 * @author Peng
 */
public class HbaseInsertDemo {

    /**
     * 创建表操作
     * 创建一个myUser表, 指定有两个列族 f1 f2
     */
    @Test
    public void createTableTest() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //指定zk的连接地址, zk里面保存了Hbase的元数据信息
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        //连接Hbase的服务器
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取Hbase的客户端
        Admin admin = connection.getAdmin();

        //通过调用valueOf的静态方法, 得到我们想要的tableName
        TableName tableName = TableName.valueOf("myUser");
        //获取hTableDescriptor
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        //添加列族
        HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("f1");
        HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("f2");

        hTableDescriptor.addFamily(hColumnDescriptor1);
        hTableDescriptor.addFamily(hColumnDescriptor2);

        //创建表操作
        admin.createTable(hTableDescriptor);
        //释放资源
        admin.close();
        connection.close();
    }

    /**
     * 向myUser中写入数据
     * put 'myUser', 'rk0001','f1:name','zhangsan'
     */
    @Test
    public void insertDataTest() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //指定zk的连接地址, zk里面保存了Hbase的元数据信息
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        //连接Hbase的服务器
        Connection connection = ConnectionFactory.createConnection(configuration);
        //指定具体的表
        TableName tableName = TableName.valueOf("myUser");
        //获取table的表对象
        Table table = connection.getTable(tableName);

        //创建put对象，最少需要一个rowKey，rowKey是字节数组类型的
        Put put = new Put(Bytes.toBytes("0001"));
        //用户基本信息
        // 列族  列  value值
        //字符串不会进行二进制转换, 但是数字会
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("zhangsan"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes("23"));
        put.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("bj"));
        put.addColumn("f1".getBytes(), "phone".getBytes(), Bytes.toBytes("1542845875"));

        //添加数据
        table.put(put);
        //释放资源
        table.close();
        connection.close();
    }

    /**
     * 批量添加数据
     */
    @Test
    public void insetBatchDataTest() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //指定zk的连接地址，zk里面保存了hbase的元数据信息
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        //连接hbase的服务器
        Connection connection = ConnectionFactory.createConnection(configuration);

        //指定具体的表
        TableName tableName = TableName.valueOf("myUser");
        //获取table的表对象
        Table table = connection.getTable(tableName);

        //创建put对象，并指定rowKey
        Put put = new Put("0002".getBytes());
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("helloWorld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("talk is cheap , show me the code"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0005".getBytes());
        put5.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("跟诸葛亮死掐"));

        Put put6 = new Put("0006".getBytes());
        put6.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<>();

        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        table.put(listPut);
        table.close();
        connection.close();
    }
}
