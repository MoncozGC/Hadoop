package com.JadePenG.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.AfterTest;

import java.io.IOException;

/**
 * 过滤器
 *
 * @author Peng
 */
public class HbaseFilterDemo {
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
     * 过滤所有rowKey小于0003的数据
     * BinaryComparator
     * RowFilter
     *
     * @throws IOException
     */
    @Test
    public void rowFilterTest() throws IOException {
        //使用范围查询, 创建scan对象
        Scan scan = new Scan();
        BinaryComparator binaryComparator = new BinaryComparator("0003".getBytes());
        //创建rowFilter对象, 传递过滤规则 <= 0003的数据, 要将rowKey传递进去, 因为rowKey是二进制字节码, 所以需要创建BinaryComparator对象
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);
        //指定过滤器, 传递过滤规则
        scan.setFilter(rowFilter);
        //执行查询 获取数据
        ResultScanner scanner = table.getScanner(scan);
        //输出查询结果
        HbasePrintlnUtil.printResultScanner(scanner);

    }

    /**
     * 列族过滤器
     * 过滤指定的列族
     * SubstringComparator
     * FamilyFilter
     *
     * @throws IOException
     */
    @Test
    public void familyFilterTest() throws IOException {
        //使用范围查询
        Scan scan = new Scan();
        SubstringComparator f2 = new SubstringComparator("f2");
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, f2);
        //传递过滤规则
        scan.setFilter(familyFilter);

        ResultScanner results = table.getScanner(scan);
        HbasePrintlnUtil.printResultScanner(results);
    }

    /**
     * 根据指定的列进行查询
     * SubstringComparator
     * QualifierFilter
     *
     * @throws IOException
     */
    @Test
    public void qualifierFilterTest() throws IOException {
        Scan scan = new Scan();
        SubstringComparator name = new SubstringComparator("name");
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, name);
        scan.setFilter(qualifierFilter);

        ResultScanner results = table.getScanner(scan);
        HbasePrintlnUtil.printResultScanner(results);
    }

    /**
     * 列值过滤器, 过滤我们的值 包含某些数据的
     * SubstringComparator
     * ValueFilter
     *
     * @throws IOException
     */
    @Test
    public void valueFilterTest() throws IOException {
        Scan scan = new Scan();
        //将所有列值包含8的列全部输出，跟列名没有关系 [模糊查询]
        SubstringComparator value = new SubstringComparator("8");
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, value);
        scan.setFilter(valueFilter);

        ResultScanner results = table.getScanner(scan);
        HbasePrintlnUtil.printResultScanner(results);
    }

    //列值过滤器与单列值过滤器的区别：列值过滤器：将包含了指定字符串的列返回
    //单列值过滤器：将匹配指定字符串的rowKey的所有的列返回
/********************************************************比较过滤器👆**********专用过滤器👇***/
    /**
     * 单列值过滤器
     * SingleColumnValueFilter会返回满足条件的整列值的所有字段
     */
    @Test
    public void singleColumnFilter() throws IOException {
        Scan scan = new Scan();
        //与SingleColumnValueFilter相反，会排除掉指定的列(刘备)，其他的列全部返回
        SingleColumnValueExcludeFilter valueExcludeFilter = new SingleColumnValueExcludeFilter("f1".getBytes(),
                "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());

        SingleColumnValueFilter valueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(),
                CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        scan.setFilter(valueExcludeFilter);

        //执行查询
        ResultScanner scanner = table.getScanner(scan);
        //输出查询结果
        HbasePrintlnUtil.printResultScanner(scanner);
    }

    /**
     * 前缀过滤器
     * 查询从00开头的所有rowKey
     *
     * @throws IOException
     */
    @Test
    public void prefixFilterText() throws IOException {
        Scan scan = new Scan();
        PrefixFilter prefixFilter = new PrefixFilter("0001".getBytes());
        scan.setFilter(prefixFilter);

        //执行查询
        ResultScanner scanner = table.getScanner(scan);
        //输出查询结果
        HbasePrintlnUtil.printResultScanner(scanner);
    }

    /**
     * 分页
     * startRow起始  endRow(下一页的起始页)
     * 001      startRow="" endRow=003
     * 002
     * 003      startRow=003 endRow=005
     * 004
     * 005      startRow=005 endRow=007
     * 006
     *
     * @throws IOException
     */
    @Test
    public void pageFilter() throws IOException {
        //页数
        int pageNum = 8;
        //每页显示几行
        int pageSize = 2;
        //第一页数据
        if (pageNum == 1) {
            //分页过滤器, 构造方法中需要传递页面的数据量
            Scan scan = new Scan();
            PageFilter pageFilter = new PageFilter(pageSize);
            //指定起始的rowKey, 给定一个空字符串 那么就是第一天数据开始
            scan.setStartRow("".getBytes());
            scan.setFilter(pageFilter);

            ResultScanner results = table.getScanner(scan);
            HbasePrintlnUtil.printResultScanner(results);
        } else {
            String startRowKey = "";
            //找到从开始到当前页的最后一条rowKey
            Scan scan = new Scan();
            scan.setStartRow("".getBytes());

            PageFilter pageFilter = new PageFilter((pageNum - 1) * pageSize + 1);
            scan.setFilter(pageFilter);

            ResultScanner results = table.getScanner(scan);
            if ("".equals(startRowKey)) {
                System.out.println("无数据");
                return;
            } else {
                for (Result result : results) {
                    byte[] row = result.getRow();
                    startRowKey = Bytes.toString(row);
                }
            }
            //获取到起始的roKew
            //System.out.println(startRowKey);
            scan.setStartRow(startRowKey.getBytes());
            pageFilter = new PageFilter(pageSize);
            scan.setFilter(pageFilter);

            results = table.getScanner(scan);
            HbasePrintlnUtil.printResultScanner(results);
        }
    }

    /**
     * 综合过滤器
     * 使用SingleColumnValueFilter查询f1列族，name为刘备的数据，并且同时满足rowKey的前缀以00开头的数据（PrefixFilter）
     *
     * @throws IOException
     */
    @Test

    public void filterListTest() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter valueFilter = new SingleColumnValueExcludeFilter("f1".getBytes(), "name".getBytes(),
                CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        FilterList filterList = new FilterList(valueFilter, prefixFilter);

        scan.setFilter(filterList);

        //执行查询
        ResultScanner scanner = table.getScanner(scan);
        //输出查询结果
        HbasePrintlnUtil.printResultScanner(scanner);
    }
}
