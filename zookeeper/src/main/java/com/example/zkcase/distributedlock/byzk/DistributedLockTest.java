package com.example.zkcase.distributedlock.byzk;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @ClassName DistributedLockTest
 * @Description TODO
 * @Author rey
 * @Date 2021/8/10
 */
public class DistributedLockTest {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        final DistributedLock lock1 = new DistributedLock();
        final DistributedLock lock2 = new DistributedLock();
        final DistributedLock lock3 = new DistributedLock();

        new Thread(new Runnable() {
            public void run() {
                try {
                    lock1.zkLock();
                    System.out.println("线程1 获取锁");
                    Thread.sleep(5 * 1000);

                    lock1.zkUnlock();
                    System.out.println("线程1 释放锁");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            public void run() {
                try {
                    lock2.zkLock();
                    System.out.println("线程2 获取锁");
                    Thread.sleep(5 * 1000);

                    lock2.zkUnlock();
                    System.out.println("线程2 释放锁");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            public void run() {
                try {
                    lock3.zkLock();
                    System.out.println("线程3 获取锁");
                    Thread.sleep(5 * 1000);

                    lock3.zkUnlock();
                    System.out.println("线程3 释放锁");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}