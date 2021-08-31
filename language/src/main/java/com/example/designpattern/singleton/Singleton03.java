package com.example.designpattern.singleton;

/**
 * @ClassName Singleton01
 * @Description  懒汉式(线程不安全)  不推荐
 * @Author rey
 * @Date 2021/8/19
 */
public class Singleton03 {
    private static Singleton03 instance;

    //构造器私有化, 外部能 new
    private Singleton03() {}

    //提供一个公有的静态方法，返回实例对象
    public static Singleton03 getInstance(){
        if(instance == null){
            instance = new Singleton03();
        }
        return instance;
    }
}