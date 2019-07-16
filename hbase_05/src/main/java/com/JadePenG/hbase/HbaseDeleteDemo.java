package com.JadePenG.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
/**
 * 删除操作
 *
 * @author Peng
 */
public class HbaseDeleteDemo {
    //创建hbase的client连接
    private Connection connection;
    private Table table;

    @BeforeTest
    public void initTableConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //指定zk的连接地址，zk里面保存了hbase的元数据信息
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        //连接hbase的服务器
        connection = ConnectionFactory.createConnection(configuration);

        //指定具体的表
        TableName tableName = TableName.valueOf("myUser");
        //获取table的表对象
        table = connection.getTable(tableName);
    }

    @AfterTest
    public void close() throws IOException {
        table.close();
        connection.close();
    }

    /**
     * 删除表数据
     */
    @Test
    public void deleteByRowKey() throws IOException {
        Delete delete = new Delete("0001".getBytes());
        delete.addColumn("f1".getBytes(), "name".getBytes());
        table.delete(delete);
        System.out.println(table);
    }

    /**
     * 删除表操作
     */
    @Test
    public void deleteTable () throws IOException {
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf("myUser"));
        admin.deleteTable(TableName.valueOf("myUser"));
    }

}
