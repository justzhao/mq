package com.zhaopeng.mq.producer.impl;

import com.zhaopeng.mq.producer.MQProducerAPI;
import com.zhaopeng.remoting.netty.NettyClient;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public class MQProducerAPIImpl implements MQProducerAPI {

    private final NettyClient nettyClient;


    public MQProducerAPIImpl(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }


}
