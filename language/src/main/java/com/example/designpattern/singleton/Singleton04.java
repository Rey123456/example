package com.example.designpattern.singleton;

/**
 * @ClassName Singleton01
 * @Description  懒汉式(线程安全，同步方法) 不推荐
 * @Author rey
 * @Date 2021/8/19
 */
public class Singleton04 {
    private static Singleton04 instance;
    private Singleton04() {}

    //提供一个公有的静态方法，返回实例对象
    public static synchronized Singleton04 getInstance(){
        if(instance == null){
            instance = new Singleton04();
        }
        return instance;
    }
}