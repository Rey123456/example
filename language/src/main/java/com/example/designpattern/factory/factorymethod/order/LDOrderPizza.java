package com.example.designpattern.factory.factorymethod.order;

import com.example.designpattern.factory.Pizza;
import com.example.designpattern.factory.factorymethod.Pizza.LDCheesePizza;
import com.example.designpattern.factory.factorymethod.Pizza.LDPepperPizza;

/**
 * @ClassName LDOrderPizza
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class LDOrderPizza extends OrderPizza{

    @Override
    Pizza createPizza(String orderType) {
        Pizza pizza = null;
        switch (orderType){
            case "cheese":
                pizza = new LDCheesePizza();
                break;
            case "pepper":
                pizza = new LDPepperPizza();
                break;
            default:
                break;
        }
        return pizza;
    }
}