package com.example.lock;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName Instance
 * @Description TODO
 * @Author rey
 * @Date 2021/8/17
 */
public class Instance {
    private static volatile Instance instance; //添加volatile关键字是为了防止指令重排序造成半初始化状态被使用

    private Instance(){

    }

    ConcurrentHashMap chm = null;

    public static Instance getInstance(){
        if(instance == null){
            synchronized (Instance.class) {
                if(instance == null){//Double Check Lock 如果没有这次dcl，可能会在1未完成时2完成了，然后1再完成一次，造成hashcode值不同
                    try {
                        Thread.sleep(1000);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    instance = new Instance();
                }
            }
        }
        return instance;
    }

    public static void main(String[] args) {
        for(int i=0;i<100;i++){
            new Thread(() -> {
                System.out.println(Instance.getInstance().hashCode());
            });
        }
    }
}