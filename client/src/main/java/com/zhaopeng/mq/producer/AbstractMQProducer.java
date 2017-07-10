package com.zhaopeng.mq.producer;

import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public abstract class AbstractMQProducer {

    protected final NettyClient nettyClient;



    public NettyClient getNettyClient() {
        return nettyClient;
    }

    protected AbstractMQProducer(NettyClientConfig nettyClientConfig) {
        this.nettyClient = new NettyClient(nettyClientConfig);

    }
}
