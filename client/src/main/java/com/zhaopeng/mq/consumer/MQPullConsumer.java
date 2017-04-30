package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.MessageInfo;
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
     * 更新offset
     *
     * @param mq
     * @param offset
     *
     * @throws MQClientException
     */
    void updateConsumeOffset(final MessageQueue mq, final long offset) throws MQClientException;


    /**
     * 查询偏移量
     *
     * @param mq
     * @param fromStore
     *
     * @return
     *
     * @throws MQClientException
     */
    long fetchConsumeOffset(final MessageQueue mq, final boolean fromStore) throws MQClientException;


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

    /**
     *
     *如果消费失败，消息会被重新发送到broker，延迟一段时间在消费
     * @param msg
     * @param delayLevel
     * @param brokerName
     * @param consumerGroup
     *
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void sendMessageBack(MessageInfo msg, int delayLevel, String brokerName, String consumerGroup)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;
}
