package com.example.designpattern.factory.abfactory.order;

import com.example.designpattern.factory.Pizza;
import com.example.designpattern.factory.factorymethod.Pizza.BJCheesePizza;
import com.example.designpattern.factory.factorymethod.Pizza.BJPepperPizza;

/**
 * @ClassName BJFactory
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class BJFactory implements AbsFactory{
    @Override
    public Pizza createPizza(String orderType) {
        Pizza pizza = null;
        switch (orderType){
            case "cheese":
                pizza = new BJCheesePizza();
                break;
            case "pepper":
                pizza = new BJPepperPizza();
                break;
            default:
                break;
        }
        return pizza;
    }
}