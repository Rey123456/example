package com.example.juc.lock.assistclassdemo;

import java.util.concurrent.CountDownLatch;

/**
 * @ClassName CountDownLatchDemo
 * @Description 6 个同学陆续离开教室后值班同学才可以关门。
 * @Author
 * @Date 2021/8/30
 */
public class CountDownLatchDemo {

    public static void main(String[] args) throws InterruptedException {

        //创建CountDownLatch对象，设置初值
        CountDownLatch countDownLatch = new CountDownLatch(6);

        for(int i = 1; i <= 6; i++){
            new Thread(() -> {
                System.out.println(Thread.currentThread().getName() + "号离开");
                countDownLatch.countDown();
            }, String.valueOf(i)).start();
        }

        countDownLatch.await();

        System.out.println(Thread.currentThread().getName() + "锁门");
    }
}