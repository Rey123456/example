package com.example.designpattern.factory.factorymethod.Pizza;

import com.example.designpattern.factory.Pizza;

/**
 * @ClassName LDPepperPizza
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class LDPepperPizza extends Pizza {
    @Override
    public void prepare() {
        setName("LD Pepper Pizza");
        System.out.println(name + "preapre");
    }
}