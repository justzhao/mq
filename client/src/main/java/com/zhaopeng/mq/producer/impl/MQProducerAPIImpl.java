package com.zhaopeng.mq.producer.impl;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.MQProducerAPI;
import com.zhaopeng.mq.producer.SendResult;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClient;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public class MQProducerAPIImpl implements MQProducerAPI {

    private final NettyClient nettyClient;


    public MQProducerAPIImpl(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }

    public SendResult send(Message msg,long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }
}
