package com.JadePenG.mr.Zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 使用JavaAPI的方式调用远端Zookeeper集群提供的功能
 * 1. 构建客户端
 * 2. 启动客户端
 * 3. 执行操作
 * 4. 关闭客户端
 */
public class ZkTest {

    @Test
    public void createPNode() throws Exception {
        //1. 构建客户端

        String strConnAdd = "node01:2181,node02:2181,node03:2181";
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(3000, 5000);
        CuratorFramework client = CuratorFrameworkFactory.newClient(strConnAdd, exponentialBackoffRetry);
        //2. 使用客户端
        client.start();

        client.create().creatingParentContainersIfNeeded()
                .withMode(CreateMode.PERSISTENT).forPath("/javaTest", "helloWord".getBytes());

    }

    private CuratorFramework client = null;

    @Before
    public void getClient() {
        //1.构建client客户端
        String strConnAdd = "node01:2181,node02:2181,node03:2181";
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(3000, 3);

        client = CuratorFrameworkFactory.newClient(strConnAdd, exponentialBackoffRetry);

        ///2.启用client客户端
        client.start();
    }

    @After
    public void closeClient() {
        //4.关闭client客户端
        client.close();
    }


    //更新(修改)操作
    @Test
    public void updateData() throws Exception {
        //3.执行我们的操作
        /*
        set path data
         */
        client.setData().forPath("/javaTest", "hello zk!".getBytes());
    }


    //获取操作
    @Test
    public void getZNodeData() throws Exception {
        //3.执行我们的操作
        /*
        get path
         */

        byte[] bytes = client.getData().forPath("/javaTest");
        System.out.println(new String(bytes));

    }

    //删除操作
    @Test
    public void DeletZNodeData() throws Exception {
        //3.执行我们的操作
        /*
        delete path
         */
        client.delete().forPath("/javaTest");

    }


    //设置watch
    @Test
    public void getZNodeWatcher() throws Exception {
        //3.执行我们的操作
        /*
        get path [watch]
         */

        TreeCache treeCache = new TreeCache(client, "/javaTest");

        treeCache.getListenable().addListener(new TreeCacheListener() {

            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {

                ChildData data = treeCacheEvent.getData();

                if (null != data) {

                    String path = data.getPath();
                    byte[] bytes = data.getData();
                    String receiveData = new String(bytes);
                    TreeCacheEvent.Type type = treeCacheEvent.getType();

                    System.out.println(path + "\t" + receiveData + "\t" + type);

                } else {

                    String path = data.getPath();
                    byte[] bytes = data.getData();
                    String receiveData = new String(bytes);
                    TreeCacheEvent.Type type = treeCacheEvent.getType();

                    System.out.println(path + "\t" + receiveData + "\t" + type);

                }

            }
        });

        //开始监听
        treeCache.start();
        Thread.sleep(50000000);


    }
}
