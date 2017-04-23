package com.zhaopeng.mq;

import com.zhaopeng.common.client.message.MessageInfo;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.query.QueryResult;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;

/**
 * Created by zhaopeng on 2017/4/23.
 */
public interface MQOperation {


    /**
     * 创建一个topic
     * @param key
     * @param newTopic
     * @param queueNum
     * @throws MQClientException
     */
    void createTopic(final String key, final String newTopic, final int queueNum)
            throws MQClientException;


    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException;



    /**
     *  在指定的时间内从一个MessageQueue 获取 消息的偏移量
     * @param mq
     * @param timestamp
     * @return
     * @throws MQClientException
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;



    /**
     * 获取messageQ的最大偏移量
     * @param mq
     * @return
     * @throws MQClientException
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;



    /**
     * 获取最小偏移量
     * @param mq
     * @return
     * @throws MQClientException
     */
    long minOffset(final MessageQueue mq) throws MQClientException;


    /**
     * 获取开始存消息的时间
     * @param mq
     * @return
     * @throws MQClientException
     */
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;



    /**
     * 根据messageId 获取消息
     * @param messageId
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    MessageInfo viewMessage(final String messageId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;



    /**
     *  查询消息
     * @param topic
     * @param key 关键字
     * @param maxNum
     * @param begin 开始时间
     * @param end 结束时间
     * @return
     * @throws MQClientException
     * @throws InterruptedException
     */
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
                             final long end) throws MQClientException, InterruptedException;

    /**   根据topic和messageId查看消息
     * @param topic
     * @param msgId
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    MessageInfo viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;
}
