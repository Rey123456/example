package com.example.lock.thread;

/**
 * @ClassName Yield_Join
 * @Description TODO
 * @Author rey
 * @Date 2021/8/18
 */
public class Yield_Join {
    //yield停一下，会到就绪状态，下一个执行的线程看线程执行规划，有可能还会回到这个线程
    static void testYield(){
        new Thread(() -> {
            for(int i=0;i<100;i++){
                System.out.println("A" + i);
                if(i%10==0) Thread.yield();
            }
        }).start();

        new Thread(() -> {
            for(int i=0; i<100;i++){
                System.out.println("--------B" + i);
                if(i%10==0) Thread.yield();
            }
        }).start();
    }

    //join等待另一个线程的结束
    static void testJoin() {
        Thread t1 = new Thread(() -> {
            for(int i=0;i<100;i++){
                System.out.println("A" + i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // if(i%10==0) Thread.yield();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                System.out.println("t2 start");
                t1.join();
                System.out.println("t1 end");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
    }

    public static void main(String[] args) {
        // testYield();
        testJoin();
    }
}