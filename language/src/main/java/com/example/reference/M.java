package com.example.reference;

/**
 * @ClassName M
 * @Description TODO
 * @Author rey
 * @Date 2021/8/17
 */
public class M {
    @Override
    protected void finalize() throws Throwable {
        System.out.println("finalize");
    }
}