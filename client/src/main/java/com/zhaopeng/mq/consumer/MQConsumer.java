package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.MessageInfo;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.mq.MQOperation;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/4/24.
 */
public interface MQConsumer extends MQOperation {



    /**
     *  注册一个消息监听器
     * @param topic
     * @param listener
     */
    void registerMessageQueueListener(final String topic, final MessageQueueListener listener);



    /**
     *
     *
     *  如果消费失败，消息会被重新发送到broker，延迟一段时间在消费
     * @param msg
     * @param delayLevel
     * @param brokerName
     *
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void sendMessageBack(final MessageInfo msg, final int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;




    /**
     * 根据topic去查询consumer的缓存 message queues
     * @param topic
     * @return
     * @throws MQClientException
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
