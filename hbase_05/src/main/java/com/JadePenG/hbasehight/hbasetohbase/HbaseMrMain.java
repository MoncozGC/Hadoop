package com.JadePenG.hbasehight.hbasetohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 读取Hbase当中的表数据写入到HBase的另外一张表当中去
 * @author Peng
 */
public class HbaseMrMain extends Configured implements Tool {

    /**
     * 组装job任务
     *
     * @param strings
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] strings) throws Exception {
        //创建job任务
        Job job = Job.getInstance(super.getConf(), "HbaseMrMain");
        //配置Job任务
        Scan scan = new Scan();
        //使用工具来初始化我们的Mapper类
        TableMapReduceUtil.initTableMapperJob(
                // 输入表
                "myUser",
                // 扫描控制器
                scan,
                // mapper class
                HbaseMapper.class,
                // key的类型
                Text.class,
                // value的类型
                Put.class,
                //将任务设置个那个job
                job);

        TableMapReduceUtil.initTableReducerJob(
                //输出的表
                "myUser2",
                // reducer class
                HbaseReducer.class,
                job);
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        //指定zk的连接地址，zk里面保存了hbase的元数据信息
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        int run = ToolRunner.run(configuration, new HbaseMrMain(), args);
        System.exit(run);
    }
}
