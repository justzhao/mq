package com.zhaopeng.mq.producer.impl;


import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.zhaopeng.common.All;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.message.SendResult;
import com.zhaopeng.common.protocol.route.BrokerData;
import com.zhaopeng.common.protocol.route.QueueData;
import com.zhaopeng.common.protocol.route.TopicRouteData;
import com.zhaopeng.mq.MQAdminClientAPIImpl;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.AbstractMQProducerOperation;
import com.zhaopeng.mq.producer.TopicPublishInfo;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public class MQProducerOperation extends AbstractMQProducerOperation {


    private static final Logger logger = LoggerFactory.getLogger(MQProducerOperation.class);

    private int times = 3;
    private final MQAdminClientAPIImpl mqAdminApi;

    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    private final Lock lockNamesrv = new ReentrantLock();
    private final static long LockTimeoutMillis = 3000;

    protected MQProducerOperation(NettyClientConfig nettyClientConfig, String addr) {
        super(nettyClientConfig);
        this.mqAdminApi = new MQAdminClientAPIImpl(nettyClient, addr);
    }


    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        TopicPublishInfo topicPublishInfo = findTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null) {

            MessageQueue mq = topicPublishInfo.selectOneMessageQueue();
            for (int i = 0; i < times; i++) {
                try {

                    String brokerAddr = findBrokerAddressInPublish(mq.getBrokerName());
                    if (null == brokerAddr) {
                        findTopicPublishInfo(mq.getTopic());
                        brokerAddr = findBrokerAddressInPublish(mq.getBrokerName());
                    }
                    SendResult result = mqAdminApi.send(brokerAddr, mq, msg, timeout);
                    return result;
                } catch (Exception e) {

                }

            }

        } else {
            throw new MQClientException("No route info of this topic, " + msg.getTopic());
        }

        return null;
    }

    private TopicPublishInfo findTopicPublishInfo(String topic) {
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        if (null == topicPublishInfo) {
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            updateTopicRouteInfoFromNameServer(topic, true);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
        return topicPublishInfo;
    }

    private boolean updateTopicRouteInfoFromNameServer(String topic, boolean isDefault) {

        try {
            if (this.lockNamesrv.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    if (isDefault) {
                        topicRouteData = mqAdminApi.getDefaultTopicRouteInfoFromNameServer(All.DEFAULT_TOPIC,
                                1000 * 3);
                        TopicInfo topicInfo = new TopicInfo();
                        if (topicRouteData != null) {

                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = data.getReadQueueNums();
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);

                                topicInfo.setReadQueueNums(queueNums);
                                topicInfo.setWriteQueueNums(queueNums);

                            }
                        }
                        // 然后再注册
                        topicInfo.setTopicName(topic);
                        mqAdminApi.createTopic(topic, topicInfo, 1000 * 3);

                    } else {
                        topicRouteData = this.mqAdminApi.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        if (changed) {
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneOne();
                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }
                            // Update Pub info
                            TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                            this.topicPublishInfoTable.put(topic, publishInfo);

                            logger.info("topicRouteTable.put TopicRouteData[{}]", cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        logger.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (Exception e) {
                    if (!topic.equals(All.DEFAULT_TOPIC)) {
                        logger.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                logger.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LockTimeoutMillis);
            }
        } catch (InterruptedException e) {
            logger.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;

    }

    private String findBrokerAddressInPublish(String brokerName) {

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(All.MASTER_ID);
        }

        return null;

    }

    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneOne();
        TopicRouteData now = nowdata.cloneOne();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        if (route != null) {

            List<BrokerData> brokerDatas = route.getBrokerDatas();
            List<QueueData> queueDatas = route.getQueueDatas();

            Map<String, QueueData> map = Maps.uniqueIndex(queueDatas, new Function<QueueData, String>() {
                @Override
                public String apply(QueueData queueData) {
                    if (queueData == null) {
                        return null;
                    }
                    return queueData.getBrokerName();
                }
            });
            for (BrokerData broker : brokerDatas) {
                QueueData qData = map.get(broker.getBrokerName());

                for (int i = 0; i < qData.getWriteQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, broker.getBrokerName(), i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        } else {
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);
            for (QueueData qd : qds) {

                BrokerData brokerData = null;
                for (BrokerData bd : route.getBrokerDatas()) {
                    if (bd.getBrokerName().equals(qd.getBrokerName())) {
                        brokerData = bd;
                        break;
                    }
                }

                if (null == brokerData) {
                    continue;
                }

                if (!brokerData.getBrokerAddrs().containsKey(All.MASTER_ID)) {
                    continue;
                }

                for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    info.getMessageQueueList().add(mq);
                }

            }

            info.setOrderTopic(false);
        }

        return info;
    }

}
