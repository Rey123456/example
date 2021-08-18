package com.example.lock.thread;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName WhatisThread
 * @Description TODO
 * @Author rey
 * @Date 2021/8/18
 */
public class WhatisThread {
    private static class T1 extends Thread{
        @Override
        public void run() {
            for(int i=0;i<10;i++){
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("T1");
            }
        }
    }

    public static void main(String[] args) {
        new T1().run(); //先执行
        new T1().start(); //与当前for循环交叉执行
        for(int i=0;i<10;i++){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("main");
        }
    }
}