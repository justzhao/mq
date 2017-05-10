package com.zhaopeng.mq.consumer.impl;

import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.mq.consumer.AbstractMQConsumer;
import com.zhaopeng.mq.consumer.MQPullConsumer;
import com.zhaopeng.mq.consumer.PullResult;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.common.RemotingUtil;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public class DefaultMQPullConsumer extends AbstractMQConsumer implements MQPullConsumer {
    // namesrv的地址
    private String addr;
    private String clientIP = RemotingUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");


    private final MQPullClientOperation mqPullClientOperation;




    public DefaultMQPullConsumer(NettyClientConfig nettyClientConfig, String addr) {
        super(nettyClientConfig);
        this.addr = addr;
        mqPullClientOperation = new MQPullClientOperation(nettyClient, addr);
    }

    public void init() {
        nettyClient.start();
    }


    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return mqPullClientOperation.pull(mq, subExpression, offset, maxNums);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        mqPullClientOperation.createTopic(key, newTopic, queueNum);
    }


    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return mqPullClientOperation.fetchSubscribeMessageQueues(topic);
    }
}
