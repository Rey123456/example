package com.example.designpattern.factory.abfactory.order;

import com.example.designpattern.factory.Pizza;

import static com.example.designpattern.factory.Util.getType;

/**
 * @ClassName OrderPizza
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class OrderPizza {
    AbsFactory absFactory;

    public OrderPizza(AbsFactory absFactory){
        this.absFactory = absFactory;

        String orderType;
        Pizza pizza = null;
        do {
            orderType = getType();
            pizza = absFactory.createPizza(orderType);
            if(pizza != null){
                pizza.prepare();
                pizza.bake();
                pizza.cut();
                pizza.box();
            }
        }while (true);
    }
}