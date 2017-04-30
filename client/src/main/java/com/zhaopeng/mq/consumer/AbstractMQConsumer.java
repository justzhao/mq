package com.zhaopeng.mq.consumer;

import com.zhaopeng.mq.consumer.impl.ClientRemotingProcessor;
import com.zhaopeng.remoting.netty.NettyClient;

/**
 * Created by zhaopeng on 2017/4/30.
 */
public abstract class AbstractMQConsumer {

    protected final NettyClient nettyClient;
    protected final ClientRemotingProcessor clientRemotingProcessor;

    protected AbstractMQConsumer(NettyClient nettyClient, ClientRemotingProcessor clientRemotingProcessor) {
        this.nettyClient = nettyClient;
        this.clientRemotingProcessor = clientRemotingProcessor;
    }
}
