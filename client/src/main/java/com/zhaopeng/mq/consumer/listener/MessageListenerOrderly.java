package com.zhaopeng.mq.consumer.listener;

import com.zhaopeng.common.client.enums.ConsumeOrderlyStatus;
import com.zhaopeng.common.client.message.MessageInfo;

import java.util.List;

/**
 * Created by zhaopeng on 2017/4/24.
 * 异步顺序消息监听器
 */
public interface MessageListenerOrderly extends MessageListener{

    ConsumeOrderlyStatus consumeMessage(final List<MessageInfo> msgs,
                                        final ConsumeOrderlyContext context);
}
