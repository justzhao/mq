package com.zhaopeng.mq.consumer;

import com.zhaopeng.remoting.netty.NettyClient;

/**
 * Created by zhaopeng on 2017/4/30.
 */
public abstract class AbstractMQClientOperation implements MQPullConsumer {

    private final NettyClient nettyClient;

    public AbstractMQClientOperation(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }

}
