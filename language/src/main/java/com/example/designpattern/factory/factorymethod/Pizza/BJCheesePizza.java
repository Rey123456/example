package com.example.designpattern.factory.factorymethod.Pizza;

import com.example.designpattern.factory.Pizza;

/**
 * @ClassName LDPepperPizza
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class BJCheesePizza extends Pizza {
    @Override
    public void prepare() {
        setName("BJ Cheese Pizza");
        System.out.println(name + "preapre");
    }
}