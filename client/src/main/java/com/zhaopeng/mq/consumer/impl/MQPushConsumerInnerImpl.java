package com.zhaopeng.mq.consumer.impl;

import com.zhaopeng.common.client.message.ConsumeFromWhere;
import com.zhaopeng.common.client.message.MessageModel;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.message.SubscriptionData;
import com.zhaopeng.mq.consumer.MQPushConsumerInner;
import com.zhaopeng.mq.consumer.RebalanceImpl;
import com.zhaopeng.mq.consumer.listener.MessageListener;
import com.zhaopeng.mq.consumer.push.RebalancePushImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/10/11.
 */
public class MQPushConsumerInnerImpl implements MQPushConsumerInner {

    private static final Logger logger = LoggerFactory.getLogger(MQPushConsumerInnerImpl.class);


    private final DefaultMQPushConsumer defaultMQPushConsumer;

    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);

    public MQPushConsumerInnerImpl(DefaultMQPushConsumer defaultMQPushConsumer,String addr) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
    }


    @Override
    public MessageModel messageModel() {
        return MessageModel.CLUSTERING;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return null;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        return null;
    }

    @Override
    public void doRebalance() {

    }

    @Override
    public void persistConsumerOffset() {

    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {

    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        return false;
    }

    @Override
    public boolean isUnitMode() {
        return false;
    }

    @Override
    public void registerMessageListener(MessageListener messageListener) {

    }

    @Override
    public void subscribe(String topic) {


        try {
            SubscriptionData subscriptionData = new SubscriptionData();
            subscriptionData.setTopic(topic);
       //     this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);

        } catch (Exception e) {

            logger.error("subscription exception {}",e);
        }


       // 2582661

    }
}
