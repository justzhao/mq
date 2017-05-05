package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.mq.MQOperation;
import com.zhaopeng.mq.exception.MQClientException;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/4/24.
 */
public interface MQConsumer extends MQOperation {



    /**
     * 根据topic去查询consumer的缓存 message queues
     * @param topic
     * @return
     * @throws MQClientException
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
