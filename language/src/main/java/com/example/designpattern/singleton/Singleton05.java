package com.example.designpattern.singleton;

/**
 * @ClassName Singleton01
 * @Description  懒汉式(线程不安全，同步代码块) 不推荐
 * @Author rey
 * @Date 2021/8/19
 */
public class Singleton05 {
    private static Singleton05 instance;
    private Singleton05() {}

    //提供一个公有的静态方法，返回实例对象
    public static Singleton05 getInstance(){
        if(instance == null){
            synchronized (Singleton05.class){
                instance = new Singleton05();
            }
        }
        return instance;
    }
}