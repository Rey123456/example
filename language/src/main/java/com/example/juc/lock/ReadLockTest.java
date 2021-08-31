package com.example.juc.lock;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @ClassName ReadLockTest
 * @Description 线程同时进行读操作，共享锁
 * @Author rey
 * @Date 2021/8/24
 */
public class ReadLockTest {
    private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    public void getbylock(Thread thread){
        rwl.readLock().lock();
        try {
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start <= 1){
                System.out.println(thread.getName()+"正在进行读操作");
            }
            System.out.println(thread.getName()+"读操作完毕");
        }finally {
            rwl.readLock().unlock();
        }
    }

    public synchronized void getbysync(Thread thread){
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start <= 1){
            System.out.println(thread.getName()+"正在进行读操作");
        }
        System.out.println(thread.getName()+"读操作完毕");
    }

    public static void main(String[] args) {
        final ReadLockTest test = new ReadLockTest();

        new Thread(() -> {
            test.getbylock(Thread.currentThread());
        }).start();

        new Thread(() -> {
            test.getbylock(Thread.currentThread());
        }).start();


        new Thread(() -> {
            test.getbysync(Thread.currentThread());
        }).start();

        new Thread(() -> {
            test.getbysync(Thread.currentThread());
        }).start();
    }
}