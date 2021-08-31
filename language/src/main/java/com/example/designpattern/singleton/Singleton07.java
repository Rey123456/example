package com.example.designpattern.singleton;

/**
 * @ClassName Singleton01
 * @Description  静态内部类
 * @Author rey
 * @Date 2021/8/19
 */
public class Singleton07 {
    private Singleton07() {}

    private static class SingletonInstance {
        private static final Singleton07 INSTANCE = new Singleton07();
    }

    //提供一个公有的静态方法，返回实例对象
    public static Singleton07 getInstance(){
        return SingletonInstance.INSTANCE;
    }
}