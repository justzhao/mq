package com.zhaopeng.mq.producer;

import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public abstract class AbstractMQProducerOperation implements MQProducer {

    protected final NettyClient nettyClient;

    protected AbstractMQProducerOperation(NettyClientConfig nettyClientConfig) {
        this.nettyClient = new NettyClient(nettyClientConfig);

    }
}
