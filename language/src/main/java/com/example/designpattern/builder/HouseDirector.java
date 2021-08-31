package com.example.designpattern.builder;

/**
 * @ClassName HouseDirector
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class HouseDirector {
    HouseBuilder houseBuilder = null;

    public HouseDirector(HouseBuilder houseBuilder){
        this.houseBuilder = houseBuilder;
    }

    public void setHouseBuilder(HouseBuilder houseBuilder) {
        this.houseBuilder = houseBuilder;
    }

    public House constructHouse(){
        return houseBuilder.buildHouse();
    }
}