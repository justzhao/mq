package com.zhaopeng.mq.producer;

import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public class AbstractMQProducer {

    protected final NettyClient nettyClient;


    protected AbstractMQProducer(NettyClientConfig nettyClientConfig) {
        this.nettyClient = new NettyClient(nettyClientConfig);

    }
}
