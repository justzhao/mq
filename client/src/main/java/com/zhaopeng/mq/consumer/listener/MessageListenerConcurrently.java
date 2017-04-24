package com.zhaopeng.mq.consumer.listener;

import com.zhaopeng.common.client.enums.ConsumeConcurrentlyStatus;
import com.zhaopeng.common.client.message.MessageInfo;

import java.util.List;

/**
 * Created by zhaopeng on 2017/4/24.
 */
public interface MessageListenerConcurrently extends MessageListener {

    ConsumeConcurrentlyStatus consumeMessage(final List<MessageInfo> msgs,
                                             final ConsumeConcurrentlyContext context);
}
