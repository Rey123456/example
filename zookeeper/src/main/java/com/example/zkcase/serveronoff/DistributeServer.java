package com.example.zkcase.serveronoff;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @ClassName DistributeServer
 * @Description TODO
 * @Author rey
 * @Date 2021/8/9
 */
public class DistributeServer {
    private static String connectString = "localhost:2181,localhost:2182,localhost:2183";//docker中起zk
    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";


    // 创建到 zk 的客户端连接
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    // 注册服务器
    public void registServer(String hostname) throws KeeperException,InterruptedException {
        String create = zk.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online " + create);
    }

    // 业务功能
    public void business(String hostname) throws InterruptedException{
        System.out.println(hostname + " is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        // 1 获取zk连接
        DistributeServer server  = new DistributeServer();
        server.getConnect();

        // 2 利用 zk 连接注册服务器信息
        server.registServer(args[0]);

        // 3 启动业务功能
        server.business(args[0]);
    }
}