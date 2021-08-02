package com.example.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @ClassName zkClient
 * @Description TODO
 * @Author rey
 * @Date 2021/8/2 下午6:33
 */

public class zkClient {
    private static String connectString = "localhost:2181,localhost:2182,localhost:2183";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    @Before
    public void init() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 收到事件通知后的回调函数(用户的业务逻辑)
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
                // 再次启动监听
                try {
                    System.out.println("---------------------------------");
                    List<String> children = zkClient.getChildren("/",true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                    System.out.println("---------------------------------");
                }catch (Exception e) {
                    e.printStackTrace();
                } }
        });
    }

    @Test
    //创建子节点
    public void create() throws KeeperException, InterruptedException {
        //参数 1:要创建的节点的路径; 参数 2:节点数据 ; 参数 3:节点权限 ; 参数 4:节点的类型
        String nodeCreated = zkClient.create("/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    //获取子节点并监听节点变化
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);

        for(String child : children){
            System.out.println(child);
        }
        //延迟阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    //判断Znode是否存在
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/test", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}