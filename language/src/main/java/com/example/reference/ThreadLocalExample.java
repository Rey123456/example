package com.example.reference;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName ThreadLocalExample
 * @Description TODO
 * @Author rey
 * @Date 2021/8/18
 */
public class ThreadLocalExample {
    //线程单独拥有
    static ThreadLocal<Person> tl = new ThreadLocal<>();

    public static void main(String[] args) {
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(tl.get());
        }).start();

        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            tl.set(new Person());
        }).start();
    }

    static class Person {
        String name = "zhangsan";
    }
}