package com.example.reference;

import java.lang.ref.WeakReference;

/**
 * @ClassName Weak
 * @Description TODO
 * @Author rey
 * @Date 2021/8/17
 */
public class Weak {
    public static void main(String[] args) {
        WeakReference<M> m = new WeakReference<>(new M());

        System.out.println(m.get());
        System.gc();
        System.out.println(m.get());

        ThreadLocal<M> tl = new ThreadLocal<>();
        tl.set(new M());
        tl.remove();
    }
}