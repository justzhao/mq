package com.zhaopeng.mq.producer.impl;


import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.AbstractMQProducerOperation;
import com.zhaopeng.mq.producer.SendResult;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public class MQProducerOperation extends AbstractMQProducerOperation {
    private final MQProducerAPIImpl mqProducerAPI;
    protected MQProducerOperation(NettyClientConfig nettyClientConfig) {
        super(nettyClientConfig);
        this.mqProducerAPI = new MQProducerAPIImpl(nettyClient);
    }



    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return mqProducerAPI.send(msg);
    }
}
