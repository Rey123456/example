package com.example.designpattern.singleton;

/**
 * @ClassName Singleton01
 * @Description  懒汉式 双重检查
 * @Author rey
 * @Date 2021/8/19
 */
public class Singleton06 {
    private static volatile Singleton06 instance;
    private Singleton06() {}

    //提供一个公有的静态方法，返回实例对象
    public static Singleton06 getInstance(){
        if(instance == null){
            synchronized (Singleton06.class){
                if(instance == null){
                    instance = new Singleton06();
                }
            }
        }
        return instance;
    }
}