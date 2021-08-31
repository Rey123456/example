package com.example.designpattern.factory.simplefactory.order;

import com.example.designpattern.factory.Pizza;
import static com.example.designpattern.factory.Util.getType;

/**
 * @ClassName OrderPizza
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class OrderPizza {
    Pizza pizza = null;
    String orderType = "";

    public OrderPizza(){
        do {
            orderType = getType();
            pizza = SimpleFactory.createPizza(orderType);
            pizza.setName(orderType);

            if(pizza != null){
                pizza.prepare();
                pizza.bake();
                pizza.cut();
                pizza.box();
            }else{
                System.out.println("failed");
                break;
            }
        }while (true);
    }



}