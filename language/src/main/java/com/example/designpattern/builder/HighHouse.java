package com.example.designpattern.builder;

/**
 * @ClassName CommonHouse
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class HighHouse extends HouseBuilder{
    @Override
    public void build() {
        System.out.println("highhouse");
    }
}