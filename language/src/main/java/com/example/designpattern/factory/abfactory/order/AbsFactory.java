package com.example.designpattern.factory.abfactory.order;

import com.example.designpattern.factory.Pizza;

/**
 * @ClassName AbsFactory
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public interface AbsFactory {
    public Pizza createPizza(String orderType);
}