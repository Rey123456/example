package com.example.lock;

/**
 * @ClassName LockTest
 * @Description TODO
 * @Author rey
 * @Date 2021/8/13
 */
public class LockTest {
    static volatile int i = 0;

    public static void n(){
        i++;
    }
    public static synchronized void m(){

    }
    public static void main(String[] args) {
        for(int j=0;j<Long.MAX_VALUE;j++){
            m();
            n();
        }
    }
}