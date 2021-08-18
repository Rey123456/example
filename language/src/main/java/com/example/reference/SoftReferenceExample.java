package com.example.reference;

import java.lang.ref.SoftReference;
/**
 * @ClassName SoftReference
 * @Description 软引用非常适合缓存使用,兼顾时间和空间的效率
 * @Author rey
 * @Date 2021/8/17
 */
public class SoftReferenceExample {
    public static void main(String[] args) {
        SoftReference<byte[]> m = new SoftReference<>(new byte[1024*1024*10]);
        System.out.println(m.get());

        System.gc();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(m.get());

        //再分配一个数组，heap将装不下，这时系统会垃圾回收，先回收一次，如果不够，会把软引用干掉
        //java8 直接报错
        byte[] b = new byte[1024*1024*15];
        System.out.println(m.get());
    }
}