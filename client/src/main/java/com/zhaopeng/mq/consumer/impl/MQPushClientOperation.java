package com.zhaopeng.mq.consumer.impl;

import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.message.PullResult;
import com.zhaopeng.mq.consumer.AbstractMQClientOperation;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClient;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/10/11.
 */
public class MQPushClientOperation extends AbstractMQClientOperation {

    public MQPushClientOperation(NettyClient nettyClient) {
        super(nettyClient);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {

    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return null;
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }
}
