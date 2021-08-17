package com.example.lock;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName HelloVolatile
 * @Description TODO
 * @Author rey
 * @Date 2021/8/13
 */
public class HelloVolatile {
    /**volatile*/ boolean running = true;

    void m(){
        System.out.println("m start");
        while (running){

        }
        System.out.println("m end");
    }

    public static void main(String[] args) {
        HelloVolatile helloVolatile = new HelloVolatile();

        new Thread(helloVolatile::m, "t1").start();

        try{
            TimeUnit.SECONDS.sleep(1);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        helloVolatile.running = false;
    }
}