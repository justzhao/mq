package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;

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












}
