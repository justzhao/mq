package com.zhaopeng.mq.producer.impl;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.AbstractMQProducer;
import com.zhaopeng.mq.producer.MQProducer;
import com.zhaopeng.mq.producer.SendResult;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public class DefaultMQProducer extends AbstractMQProducer implements MQProducer {

    private volatile int defaultTopicQueueNums = 4;
    private int sendMsgTimeout = 3000;
    private int compressMsgBodyOverHowmuch = 1024 * 4;
    private int retryTimesWhenSendFailed = 2;
    private int retryTimesWhenSendAsyncFailed = 2;

    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    private final MQProducerOperation mqProducerOperation;

    public DefaultMQProducer(NettyClientConfig nettyClientConfig) {
        super(nettyClientConfig);
        this.mqProducerOperation = new MQProducerOperation(nettyClientConfig);
    }


    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return mqProducerOperation.send(msg);
    }
}
