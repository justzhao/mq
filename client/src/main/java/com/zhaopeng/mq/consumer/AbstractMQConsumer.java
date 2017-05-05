package com.zhaopeng.mq.consumer;


import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;

/**
 * Created by zhaopeng on 2017/4/30.
 */
public abstract class AbstractMQConsumer {

    protected final NettyClient nettyClient;


    protected AbstractMQConsumer(NettyClientConfig nettyClientConfig) {
        this.nettyClient = new NettyClient(nettyClientConfig);

    }
}
