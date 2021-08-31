package com.example.designpattern.singleton;

/**
 * @ClassName Singleton01
 * @Description  枚举
 * @Author rey
 * @Date 2021/8/19
 */
public class Singleton08 {
    public static void main(String[] args) {
        Singleton instance = Singleton.INSTANCE;
        Singleton instance2 = Singleton.INSTANCE;
        System.out.println(instance == instance2);
        System.out.println(instance.hashCode());
        System.out.println(instance2.hashCode());
        instance.sayOK();
    }
}

enum Singleton{
    INSTANCE;
    public void sayOK(){
        System.out.println("ok..");
    }
}