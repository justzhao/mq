package com.zhaopeng.mq.consumer.listener;

import com.zhaopeng.common.client.message.MessageQueue;

/**
 * Created by zhaopeng on 2017/4/24.
 */
public class ConsumeConcurrentlyContext {

    private final MessageQueue messageQueue;
    // 消息队列重试策略
    private int delayLevelWhenNextConsume = 0;
    private int ackIndex = Integer.MAX_VALUE;

    public ConsumeConcurrentlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public int getDelayLevelWhenNextConsume() {
        return delayLevelWhenNextConsume;
    }

    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }

    public int getAckIndex() {
        return ackIndex;
    }

    public void setAckIndex(int ackIndex) {
        this.ackIndex = ackIndex;
    }
}
