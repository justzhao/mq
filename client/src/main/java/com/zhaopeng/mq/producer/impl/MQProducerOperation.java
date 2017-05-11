package com.zhaopeng.mq.producer.impl;


import com.zhaopeng.common.All;
import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.AbstractMQProducerOperation;
import com.zhaopeng.mq.producer.SendResult;
import com.zhaopeng.mq.producer.TopicPublishInfo;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public class MQProducerOperation extends AbstractMQProducerOperation {

    private int times = 3;
    private final MQProducerAPIImpl mqProducerAPI;

    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<>();

    protected MQProducerOperation(NettyClientConfig nettyClientConfig) {
        super(nettyClientConfig);
        this.mqProducerAPI = new MQProducerAPIImpl(nettyClient);
    }


    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        TopicPublishInfo topicPublishInfo = findTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.isHaveTopicRouterInfo()) {

            MessageQueue mq = topicPublishInfo.selectOneMessageQueue();
            for (int i = 0; i < times; i++) {
                try {

                    String brokerAddr = findBrokerAddressInPublish(mq.getBrokerName());
                    if (null == brokerAddr) {
                        findTopicPublishInfo(mq.getTopic());
                        brokerAddr = findBrokerAddressInPublish(mq.getBrokerName());
                    }
                    SendResult result = mqProducerAPI.send(mq,topicPublishInfo,msg, timeout);
                    return result;
                } catch (Exception e) {

                }

            }

        } else {
            throw new MQClientException("No route info of this topic, " + msg.getTopic());
        }

        return null;
    }

    private TopicPublishInfo findTopicPublishInfo(String topic) {


        // 更新 brokerAddrTable

        return null;
    }


    private String findBrokerAddressInPublish(String brokerName){

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(All.MASTER_ID);
        }

        return null;

    }

}
