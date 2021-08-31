package com.example.designpattern.singleton;

/**
 * @ClassName Singleton01
 * @Description  饿汉式(静态代码块)
 * @Author rey
 * @Date 2021/8/19
 */
public class Singleton02 {
    //构造器私有化, 外部能 new
    private Singleton02() {

    }

    private static Singleton02 instance;

    // 在静态代码块中，创建单例对象
    static {
        instance = new Singleton02();
    }

    //提供一个公有的静态方法，返回实例对象
    public static Singleton02 getInstance(){
        return instance;
    }
}