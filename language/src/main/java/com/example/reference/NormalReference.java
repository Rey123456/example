package com.example.reference;

import java.io.IOException;

/**
 * @ClassName NormalReference
 * @Description TODO
 * @Author rey
 * @Date 2021/8/17
 */
public class NormalReference {
    public static void main(String[] args) throws IOException {
        M m = new M();
        m = null;
        System.gc();

        System.in.read();
    }
}