package com.example.juc.lock;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName ThreadCommunication1
 * @Description A 线程打印 5 次 A，B 线程打印 10 次 B，C 线程打印 15 次 C,按照 此顺序循环 10 轮
 * @Author rey
 * @Date 2021/8/24
 */

public class ThreadCommunication1 {
    private int flag = 0; // 1 AA 2 BB 3CC
    private ReentrantLock lock = new ReentrantLock();

    private Condition cd1 = lock.newCondition(); //A
    private Condition cd2 = lock.newCondition();//B
    private Condition cd3 = lock.newCondition();//C

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public void printA5(int loop) throws InterruptedException {
        lock.lock();
        try {
            while(flag != 1){
                cd1.await();
            }
            for(int i=0;i<5;i++){
                System.out.println(Thread.currentThread().getName() + "::" + i + "次，第 "+ loop + "轮");
            }
            flag = 2;
            cd2.signal();
        }finally {
            lock.unlock();
        }
    }

    public void printB6(int loop) throws InterruptedException {
        lock.lock();
        try {
            while(flag != 2){
                cd2.await();
            }
            for(int i=0;i<6;i++){
                System.out.println(Thread.currentThread().getName() + "::" + i + "次，第 "+ loop + "轮");
            }
            flag = 3;
            cd3.signal();
        }finally {
            lock.unlock();
        }
    }

    public void printC7(int loop) throws InterruptedException {
        lock.lock();
        try {
            while(flag != 3){
                cd3.await();
            }
            for(int i=0;i<7;i++){
                System.out.println(Thread.currentThread().getName() + "::" + i + "次，第 "+ loop + "轮");
            }
            flag = 1;
            cd1.signal();
        }finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) {
        ThreadCommunication1 comm = new ThreadCommunication1();
        comm.setFlag(1);

        new Thread(() -> {
            for(int i=0;i<5;i++){
                try {
                    comm.printA5(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "A").start();

        new Thread(() -> {
            for(int i=0;i<5;i++){
                try {
                    comm.printB6(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "B").start();

        new Thread(() -> {
            for(int i=0;i<5;i++){
                try {
                    comm.printC7(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "C").start();
    }
}