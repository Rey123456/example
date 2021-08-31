package com.example.designpattern.factory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @ClassName Util
 * @Description
 * @Author
 * @Date 2021/8/25
 */
public class Util {

    // 写一个方法，可以获取客户希望订购的披萨种类
    public static String getType() {
        try {
            BufferedReader strin = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("input pizza 种类:");
            String str = strin.readLine();
            return str;
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }
}