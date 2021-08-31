package com.example.designpattern.factory.factorymethod.order;

import com.example.designpattern.factory.Pizza;
import static com.example.designpattern.factory.Util.getType;

/**
 * @ClassName OrderPizza
 * @Description
 * @Author rey
 * @Date 2021/8/25
 */
public abstract class OrderPizza {
    abstract Pizza createPizza(String orderType);

    public OrderPizza() {
        String orderType;
        Pizza pizza = null;
        do {
            orderType = getType();
            pizza = createPizza(orderType);

            pizza.prepare();
            pizza.bake();
            pizza.cut();
            pizza.box();

        }while (true);
    }


}