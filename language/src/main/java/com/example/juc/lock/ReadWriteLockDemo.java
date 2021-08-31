package com.example.juc.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @ClassName ReadWriteLockDemo
 * @Description TODO
 * @Author
 * @Date 2021/8/31
 */
//资源类
class MyCache{
    //创建map集合
    private volatile Map<String, Object> map = new HashMap<>();
    //创建读写锁对象
    private ReadWriteLock rwlock = new ReentrantReadWriteLock();

    //放数据
    public void put(String key, Object value){
        rwlock.writeLock().lock();
        try {
            System.out.println(Thread.currentThread().getName() + "writing");
            TimeUnit.MICROSECONDS.sleep(300);
            map.put(key, value);
            System.out.println(Thread.currentThread().getName() + "write over");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            rwlock.writeLock().unlock();
        }
    }

    //取数据
    public Object get(String key){
        rwlock.readLock().lock();
        Object value = null;
        try {
            System.out.println(Thread.currentThread().getName() + "getting");
            TimeUnit.MICROSECONDS.sleep(300);
            value = map.get(key);
            System.out.println(Thread.currentThread().getName() + "get over");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            rwlock.readLock().unlock();
        }
        return value;
    }
}
public class ReadWriteLockDemo {
    public static void main(String[] args) {
        MyCache myCache = new MyCache();

        //创建线程放数据
        for(int i=1;i<=5;i++){
            final int num = i;
            new Thread(() -> {
                myCache.put(num+"", num);
            }, String.valueOf(i)).start();
        }

        //创建线程取数据
        for(int i=1;i<=5;i++){
            final int num = i;
            new Thread(() -> {
                System.out.println(myCache.get(num+""));
            }, String.valueOf(i)).start();
        }
    }
}