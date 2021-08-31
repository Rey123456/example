package com.example.designpattern.factory.simplefactory.pizza;

import com.example.designpattern.factory.Pizza;

/**
 * @ClassName PepperPizza
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class PepperPizza extends Pizza {

    @Override
    public void prepare() {
        System.out.println(" 给胡椒披萨准备原材料 ");
    }
}