package com.example.designpattern.factory.factorymethod.order;

/**
 * @ClassName PizzaStore
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class PizzaStore {
    public static void main(String[] args) {
        String loc = args[0];
        if(loc.equals("bj")){
            new BJOrderPizza();
        }else if(loc.equals("ld")){
            new LDOrderPizza();
        }else {
            System.out.println("no loc for input");
        }
    }
}