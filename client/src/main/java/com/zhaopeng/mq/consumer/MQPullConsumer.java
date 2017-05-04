package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public interface MQPullConsumer extends MQConsumer {





    /**
     * 拉取一条消息，不会阻塞
     * @param mq
     * @param subExpression  子标签
     * @param offset 偏移量
     * @param maxNums  拉取的最大个数
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset,
                    final int maxNums) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;


    /**
     * Pulling the messages in the specified timeout
     *
     * @param mq
     * @param subExpression
     * @param offset
     * @param maxNums
     * @param timeout
     *
     * @return
     *
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */

    /**
     *  拉取消息，指定超时时间
     * @param mq
     * @param subExpression
     * @param offset
     * @param maxNums
     * @param timeout
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset,
                    final int maxNums, final long timeout) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;


    /**
     *
     * 异步拉取消息
     *
     * @param mq
     * @param subExpression
     * @param offset
     * @param maxNums
     * @param pullCallback
     *
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
              final PullCallback pullCallback) throws MQClientException, RemotingException,
            InterruptedException;

    /**
     * 异步拉取消息
     *
     * @param mq
     * @param subExpression
     * @param offset
     * @param maxNums
     * @param pullCallback
     * @param timeout
     *
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
              final PullCallback pullCallback, long timeout) throws MQClientException, RemotingException,
            InterruptedException;




    /**
     * 根据topic查询 messageQueue
     *
     * @param topic
     *         message topic
     *
     * @return message queue set
     *
     * @throws MQClientException
     */
    Set<MessageQueue> fetchMessageQueuesInBalance(final String topic) throws MQClientException;


}
