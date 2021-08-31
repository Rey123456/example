package com.example.juc.lock;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName ThreadCommunication
 * @Description 一个线程对当前数值加1，另一个线程对当前数值减1,要求用线程间通信
 * @Author rey
 * @Date 2021/8/24
 */

class SharebyLock{
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private int number = 0;

    public void incr() throws InterruptedException {
        lock.lock();
        try{
            while (number!=0){
                condition.await();
            }
            number++;
            System.out.println(Thread.currentThread().getName() + "::" + number);

            condition.signalAll();
        }finally {
            lock.unlock();
        }
    }

    public void desc() throws InterruptedException {
        lock.lock();
        try{
            while (number!=1){
                condition.await();
            }
            number--;
            System.out.println(Thread.currentThread().getName() + "::" + number);

            condition.signalAll();
        }finally {
            lock.unlock();
        }
    }
}

class SharebySync{
    //初始值
    private int number = 0;

    public synchronized void incr() throws InterruptedException {
        //第二步 判断 干活 通知
        while(number!=0){
            this.wait(); //在哪里睡，就在哪里醒,所以使用if的话会存在虚假唤醒的现象，应使用while再次判断number值
        }
        //如果number值是0，就+1操作
        number++;
        System.out.println(Thread.currentThread().getName() + "::" + number);
        //通知其他线程
        this.notifyAll();
    }

    public synchronized void desc() throws InterruptedException {
        while(number!=1){
            this.wait();
        }
        number--;
        System.out.println(Thread.currentThread().getName() + "::" + number);
        this.notifyAll();
    }
}

public class ThreadCommunication {
    //第三步 创建多个线程，调用资源类的操作方法
    public static void main(String[] args) {
        // SharebySync share = new SharebySync();
        SharebyLock share = new SharebyLock();

        new Thread(() -> {
            try {
                for (int i = 1; i <=10; i++) {
                    share.incr();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "AA").start();

        new Thread(() -> {
            try {
                for (int i = 1; i <=10; i++) {
                    share.desc();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "BB").start();

        new Thread(() -> {
            try {
                for (int i = 1; i <=10; i++) {
                    share.incr();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "CC").start();

        new Thread(() -> {
            try {
                for (int i = 1; i <=10; i++) {
                    share.desc();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "DD").start();
    }
}