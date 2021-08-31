package com.example.designpattern.singleton;

/**
 * @ClassName Singleton01
 * @Description 饿汉式(静态常量)
 * @Author rey
 * @Date 2021/8/19
 */
public class Singleton01 {
    //构造器私有化, 外部能 new
    private Singleton01() {

    }

    //内部创建对象实例
    private final static Singleton01 instance = new Singleton01();

    //提供一个公有的静态方法，返回实例对象
    public static Singleton01 getInstance(){
        return instance;
    }
}