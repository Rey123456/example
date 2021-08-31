package com.example.designpattern.builder;

/**
 * @ClassName Client
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class Client {
    public static void main(String[] args) {
        CommonHouse commonHouse = new CommonHouse();
        HouseDirector houseDirector = new HouseDirector(commonHouse);
        House house = houseDirector.constructHouse();

        System.out.println("-----------");

        HighHouse highHouse = new HighHouse();
        houseDirector.setHouseBuilder(highHouse);
        houseDirector.constructHouse();
    }
}