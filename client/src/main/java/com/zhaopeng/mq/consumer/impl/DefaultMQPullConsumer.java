package com.zhaopeng.mq.consumer.impl;

import com.zhaopeng.common.client.message.MessageInfo;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.query.QueryResult;
import com.zhaopeng.mq.consumer.*;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClient;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public class DefaultMQPullConsumer extends AbstractMQConsumer implements MQPullConsumer {


    private final MQPullClientOperation mqPullClientOperation;


    /**
     * Offset Storage
     */
   // private OffsetStore offsetStore;
    /**
     * Topic set you want to register
     */
    private Set<String> registerTopics = new HashSet<>();


    private MessageQueueListener messageQueueListener;


    public DefaultMQPullConsumer(NettyClient nettyClient, ClientRemotingProcessor clientRemotingProcessor) {
        super(nettyClient, clientRemotingProcessor);
        mqPullClientOperation = new MQPullClientOperation(nettyClient);
    }


    @Override
    public void registerMessageQueueListener(String topic, MessageQueueListener listener) {

        synchronized (this.registerTopics) {
            this.registerTopics.add(topic);
            if (listener != null) {
                this.messageQueueListener = listener;
            }
        }

    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return mqPullClientOperation.pull(mq, subExpression, offset, maxNums);
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return mqPullClientOperation.pull(mq, subExpression, offset, maxNums,timeout);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {
         mqPullClientOperation.pull(mq, subExpression, offset, maxNums,pullCallback);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        mqPullClientOperation.pull(mq, subExpression, offset, maxNums,pullCallback,timeout);
    }



    @Override
    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {

    }

    @Override
    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        return 0;
    }

    @Override
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        return null;
    }

    @Override
    public void sendMessageBack(MessageInfo msg, int delayLevel, String brokerName, String consumerGroup) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {


    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {

    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return 0;
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return 0;
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return 0;
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return 0;
    }

    @Override
    public MessageInfo viewMessage(String messageId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return null;
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
        return null;
    }

    @Override
    public MessageInfo viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return null;
    }

    @Override
    public void sendMessageBack(MessageInfo msg, int delayLevel, String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return null;
    }
}
