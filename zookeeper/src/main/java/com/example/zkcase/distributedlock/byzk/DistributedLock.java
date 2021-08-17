package com.example.zkcase.distributedlock.byzk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ClassName DistributedLock
 * @Description TODO
 * @Author rey
 * @Date 2021/8/10
 */
public class DistributedLock {
    private static String connectString = "localhost:2181,localhost:2182,localhost:2183";//docker中起zk
    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;

    private String rootNode = "locks";
    private String subNode = "seq-";
    private String waitPath; // 当前 client 等待的子节点

    //ZooKeeper 连接
    private CountDownLatch connectLatch = new CountDownLatch(1);
    //ZooKeeper 节点等待
    private CountDownLatch waitLatch = new CountDownLatch(1);
    // 当前 client 创建的子节点
    private String currentNode;

    // 和 zk 服务建立连接，并创建根节点
    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                // 连接建立时, 打开 latch, 唤醒 wait 在该 latch 上的线程
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                }
                //发生了 waitPath 的删除事件
                if(watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)){
                    waitLatch.countDown();
                }
            }
        });

        // 等待连接建立
        connectLatch.await();

        //获取根结点状态
        Stat stat = zk.exists("/" + rootNode, false);

        //如果根节点不存在，则创建根节点，根节点类型为永久节点
        if(stat == null){
            System.out.println("根节点不存在");
            zk.create("/" + rootNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    //加锁方法
    public void zkLock() throws KeeperException, InterruptedException {
        //在根节点下创建临时顺序节点，返回值为创建的节点路径
        currentNode = zk.create("/" + rootNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // wait 一小会, 让结果更清晰一些
        Thread.sleep(10);

        // 注意, 没有必要监听"/locks"的子节点的变化情况
        List<String> childrenNodes = zk.getChildren("/" + rootNode, false);
        if(childrenNodes.size() == 1) { //只有我一个节点，获得锁
            return;
        }else{
            Collections.sort(childrenNodes);
            String thisNode = currentNode.substring(("/" + rootNode + "/").length());
            int index = childrenNodes.indexOf(thisNode);

            if(index == -1){
                System.out.println("数据异常");
            }else if(index == 0){
                //说明当前节点就是最小的
                return;
            }else{
                //前一节点
                this.waitPath = "/" + rootNode + "/" + childrenNodes.get(index - 1);
                //注册监听
                zk.getData(waitPath, true, new Stat());
                //进入等待状态
                waitLatch.await();

                if(turn()) return;
            }
        }
    }

    //当前一节点被删除后的判断逻辑
    public boolean turn() throws KeeperException, InterruptedException {
        List<String> childrenNodes = zk.getChildren("/" + rootNode, false);
        Collections.sort(childrenNodes);
        String thisNode = currentNode.substring(("/" + rootNode + "/").length());
        int index = childrenNodes.indexOf(thisNode);

        if(index == -1){
            System.out.println("数据异常");
            return false;
        }else if(index == 0){
            //说明当前节点就是最小的
            return true;
        }else{
            //前一节点
            this.waitPath = "/" + rootNode + "/" + childrenNodes.get(index - 1);
            //注册监听
            zk.getData(waitPath, true, new Stat());
            //进入等待状态
            waitLatch.await();

            return turn();
        }
    }

    //解锁方法
    public void zkUnlock() throws KeeperException, InterruptedException {
        zk.delete(this.currentNode, -1);
    }
}