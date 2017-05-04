package com.zhaopeng.mq.consumer.impl;


import com.google.common.base.Strings;
import com.zhaopeng.common.All;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.message.MessageInfo;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.query.QueryResult;
import com.zhaopeng.common.protocol.route.BrokerData;
import com.zhaopeng.common.protocol.route.TopicRouteData;
import com.zhaopeng.mq.consumer.AbstractMQClientOperation;
import com.zhaopeng.mq.consumer.PullCallback;
import com.zhaopeng.mq.consumer.PullResult;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Created by zhaopeng on 2017/4/30.
 */
public class MQPullClientOperation extends AbstractMQClientOperation {

    private static final Logger logger = LoggerFactory.getLogger(MQPullClientOperation.class);

    private final long bootTime = System.currentTimeMillis();


    private final MQPullClientAPIImpl mqPullClientAPI;

    private long timeoutMillis = 6000;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQPullClientOperationThread");
        }
    });


    public MQPullClientOperation(NettyClient nettyClient) {
        super(nettyClient);
        mqPullClientAPI = new MQPullClientAPIImpl(nettyClient);
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {

    }




    @Override
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        return null;
    }




    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {

        try {
            TopicRouteData topicRouteData = mqPullClientAPI.getTopicRouteInfoFromNameServer(key, timeoutMillis);
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            if (brokerDataList != null && !brokerDataList.isEmpty()) {

                Collections.sort(brokerDataList);

                boolean createOKAtLeastOnce = false;
                MQClientException exception = null;

                StringBuilder orderTopicString = new StringBuilder();

                for (BrokerData brokerData : brokerDataList) {
                    String addr = brokerData.getBrokerAddrs().get(All.MASTER_ID);
                    if (addr != null) {
                        TopicInfo topicInfo = new TopicInfo(newTopic);
                        topicInfo.setReadQueueNums(queueNum);
                        topicInfo.setWriteQueueNums(queueNum);
                        boolean createOK = false;
                        // 重试五次
                        for (int i = 0; i < 5; i++) {
                            try {
                                mqPullClientAPI.createTopic(addr, key, topicInfo, timeoutMillis);
                                createOK = true;
                                createOKAtLeastOnce = true;
                                break;
                            } catch (Exception e) {
                                if (4 == i) {
                                    exception = new MQClientException("create topic to broker exception", e);
                                }
                            }
                        }

                        if (createOK) {
                            orderTopicString.append(brokerData.getBrokerName());
                            orderTopicString.append(":");
                            orderTopicString.append(queueNum);
                            orderTopicString.append(";");
                        }
                    }
                }

                if (exception != null && !createOKAtLeastOnce) {
                    throw exception;
                }
            } else {
                throw new MQClientException("Not found broker, maybe key is wrong", null);
            }
        } catch (Exception e) {
            throw new MQClientException("create new topic failed", e);
        }

    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {

        String brokerAddr = mqPullClientAPI.getBrokerAddrByName(mq.getBrokerName());
        if (null == brokerAddr) {
            // 重新根据topic获取addr

            mqPullClientAPI.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = mqPullClientAPI.getBrokerAddrByName(mq.getBrokerName());


        }

        if (brokerAddr != null) {
            try {
                return mqPullClientAPI.searchOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timestamp,
                        timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
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
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        if (Strings.isNullOrEmpty(topic)) return null;
        return mqPullClientAPI.fetchSubscribeMessageQueues(topic);
    }


}
