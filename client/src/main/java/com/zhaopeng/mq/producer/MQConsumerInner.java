
package com.zhaopeng.mq.producer;

import com.zhaopeng.common.client.message.ConsumeFromWhere;
import com.zhaopeng.common.client.message.MessageModel;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.message.SubscriptionData;

import java.util.Set;

/**
 * Consumer inner interface
 */
public interface MQConsumerInner {
    String groupName();

    MessageModel messageModel();



    ConsumeFromWhere consumeFromWhere();

    Set<SubscriptionData> subscriptions();

    void doRebalance();

    void persistConsumerOffset();

    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();


}
