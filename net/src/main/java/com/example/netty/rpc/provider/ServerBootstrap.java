package com.example.netty.rpc.provider;

import com.example.netty.rpc.netty.NettyServer;

/**
 * @ClassName ServerBootstrap
 * @Description TODO
 * @Author rey
 * @Date 2021/8/9
 */
public class ServerBootstrap {
    public static void main(String[] args) {
        NettyServer.startServer("127.0.0.1", 7000);
    }
}