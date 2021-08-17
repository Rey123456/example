package com.example.netty.rpc.provider;

import com.example.netty.rpc.pulicinterface.HelloService;

/**
 * @ClassName HelloServiceImpl
 * @Description TODO
 * @Author rey
 * @Date 2021/8/9
 */
public class HelloServiceImpl implements HelloService {
    private static int count = 0;

    public String Hello(String mes) {
        System.out.println("收到客户端消息=" + mes);
        //根据 mes 返回不同的结果
        if(mes != null) {
            return "你好客户端, 我已经收到你的消息 [" + mes + "] 第" + (++count) + " 次";
        } else {
            return "你好客户端, 我已经收到你的消息 ";
        }
    }
}