package com.example.designpattern.factory.simplefactory.order;

import com.example.designpattern.factory.simplefactory.pizza.CheesePizza;
import com.example.designpattern.factory.simplefactory.pizza.GreekPizza;
import com.example.designpattern.factory.simplefactory.pizza.PepperPizza;
import com.example.designpattern.factory.Pizza;

/**
 * @ClassName SimpleFactory
 * @Description
 * 看一个披萨的项目:要便于披萨种类的扩展，要便于维护
 * 1) 披萨的种类很多(比如 GreekPizz、CheesePizz 等)
 * 2) 披萨的制作有 prepare，bake, cut, box
 * 3) 完成披萨店订购功能。
 * @Author
 * @Date 2021/8/25
 */
public class SimpleFactory {

    //简单工厂模式 也叫 静态工厂模式
    public static Pizza createPizza(String orderType){
    // public Pizza createPizza(String orderType){
        Pizza pizza = null;
        System.out.println("simpleFactory");
        switch (orderType){
            case "greek":
                pizza = new GreekPizza();
                break;
            case "cheese":
                pizza = new CheesePizza();
                break;
            case "pepper":
                pizza = new PepperPizza();
                break;
            default:
                break;
        }
        return pizza;
    }
}