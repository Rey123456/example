package com.example.juc.lock.assistclassdemo;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName CyclicBarrierDemo
 * @Description 集齐 7 颗龙珠就可以召唤神龙
 * @Author
 * @Date 2021/8/30
 */
public class CyclicBarrierDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(7, () -> {
            System.out.println("集齐 7 颗龙珠就可以召唤神龙");
        });

        for(int i=1;i<=7;i++){
            new Thread(() -> {
                try {
                    System.out.println(Thread.currentThread().getName() + "颗龙珠拿到");
                    cyclicBarrier.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }, String.valueOf(i)).start();
        }
    }
}