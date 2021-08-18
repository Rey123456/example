package com.example.lock.thread;

/**
 * @ClassName HowtoCreateThread
 * @Description TODO
 * @Author rey
 * @Date 2021/8/18
 */
public class HowtoCreateThread {
    static class MyThread extends Thread {
        @Override
        public void run() {
            System.out.println("thread");
        }
    }

    static class MyRun implements Runnable {
        @Override
        public void run() {
            System.out.println("run");
        }
    }

    public static void main(String[] args) {
        new MyThread().start();
        new Thread(new MyRun()).start();
        new Thread(() -> {
            System.out.println("lambda");
        }).start();
    }
}