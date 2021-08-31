package com.example.designpattern.builder;

/**
 * @ClassName HouseBuilder
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public abstract class HouseBuilder {
    protected House house = new House();

    public abstract void build();

    public House buildHouse(){
        build();
        return house;
    }
}