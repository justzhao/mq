package com.zhaopeng.mq.producer.impl;


import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.AbstractMQProducerOperation;
import com.zhaopeng.mq.producer.SendResult;
import com.zhaopeng.mq.producer.TopicPublishInfo;
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
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        TopicPublishInfo topicPublishInfo = findTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.isHaveTopicRouterInfo()) {

        } else {
            throw new MQClientException("No route info of this topic, " + msg.getTopic());
        }

        return mqProducerAPI.send(msg, timeout);
    }

    private TopicPublishInfo findTopicPublishInfo(String topic) {
        return null;
    }


}
