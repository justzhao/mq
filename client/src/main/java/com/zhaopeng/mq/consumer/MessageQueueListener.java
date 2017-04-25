package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.MessageQueue;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public interface MessageQueueListener {


    /**
     *
     * @param topic
     * @param mqAll
     * @param mqDivided 分配给当前消费者的MessageQueue
     */
    void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
                             final Set<MessageQueue> mqDivided);
}
