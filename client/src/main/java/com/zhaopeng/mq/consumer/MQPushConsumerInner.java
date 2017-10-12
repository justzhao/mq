package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.ConsumeFromWhere;
import com.zhaopeng.common.client.message.MessageModel;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.message.SubscriptionData;
import com.zhaopeng.mq.consumer.listener.MessageListener;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/10/11.
 */
public interface MQPushConsumerInner {


    MessageModel messageModel();


    ConsumeFromWhere consumeFromWhere();

    Set<SubscriptionData> subscriptions();

    void doRebalance();

    void persistConsumerOffset();

    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();

    public void registerMessageListener(MessageListener messageListener) ;

}
