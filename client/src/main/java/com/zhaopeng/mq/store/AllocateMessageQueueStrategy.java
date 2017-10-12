
package com.zhaopeng.mq.store;


import com.zhaopeng.common.client.message.MessageQueue;

import java.util.List;

/**
 * Strategy Algorithm for message allocating between consumers
 */
public interface AllocateMessageQueueStrategy {

    /**
     *  根据客户端id来分配
     *
     * @param consumerGroup 当前分组
     * @param currentCID 当前客户端id
     * @param mqAll 当前topic所有的队列
     * @param cidAll 当前分组所有的客户端
     * @return 返回分配的队列
     */

    List<MessageQueue> allocate(
            final String consumerGroup,
            final String currentCID,
            final List<MessageQueue> mqAll,
            final List<String> cidAll
    );

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getName();
}
