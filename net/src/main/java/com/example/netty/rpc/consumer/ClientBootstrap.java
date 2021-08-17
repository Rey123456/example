package com.example.netty.rpc.consumer;

import com.example.netty.rpc.netty.NettyClient;
import com.example.netty.rpc.netty.NettyServerHandler;
import com.example.netty.rpc.pulicinterface.HelloService;

/**
 * @ClassName ClientBootstrap
 * @Description TODO
 * @Author rey
 * @Date 2021/8/9
 */
public class ClientBootstrap {
    public static void main(String[] args) throws InterruptedException {
        NettyClient consumer = new NettyClient();

        HelloService service = (HelloService)consumer.getBean(HelloService.class, NettyServerHandler.providerName);

        while(true){
            System.out.println("-----------------------");
            Thread.sleep(2 * 1000);
            //通过代理对象调用服务提供者的方法(服务)
            String res = service.Hello("你好 dubbo~");
            System.out.println("调用的结果 res= " + res);
            System.out.println("........................");
        }
    }
}