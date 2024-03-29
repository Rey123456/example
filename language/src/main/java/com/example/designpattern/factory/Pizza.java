package com.example.designpattern.factory;

/**
 * @ClassName Pizza
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public abstract class Pizza {
    protected String name;

    public abstract void prepare();


    public void bake() {
        System.out.println(name + " baking;");
    }

    public void cut() {
        System.out.println(name + " cutting;");
    }

    public void box() {
        System.out.println(name + " boxing;");
    }

    public void setName(String name) {
        this.name = name;
    }
}