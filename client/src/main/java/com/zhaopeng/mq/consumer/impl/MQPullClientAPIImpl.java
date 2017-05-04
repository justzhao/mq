package com.zhaopeng.mq.consumer.impl;

import com.google.common.base.Strings;
import com.zhaopeng.common.All;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.protocol.RequestCode;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.SearchOffsetRequest;
import com.zhaopeng.common.protocol.body.SearchOffsetResponse;
import com.zhaopeng.common.protocol.route.BrokerData;
import com.zhaopeng.common.protocol.route.QueueData;
import com.zhaopeng.common.protocol.route.TopicRouteData;
import com.zhaopeng.mq.consumer.MQPullClientAPI;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.protocol.JsonSerializable;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhaopeng on 2017/5/2.
 */
public class MQPullClientAPIImpl implements MQPullClientAPI {
    private static final Logger logger = LoggerFactory.getLogger(MQPullClientAPIImpl.class);


    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();

    private long timeoutMillis = 6000;
    private long LockTimeoutMillis = 3000;

    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    protected final NettyClient nettyClient;

    public MQPullClientAPIImpl(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }

    long searchOffset(final String addr, final String topic, final int queueId, final long timestamp, final long timeoutMillis) {
        SearchOffsetRequest searchOffset = new SearchOffsetRequest();
        searchOffset.setTopic(topic);
        searchOffset.setQueueId(queueId);
        searchOffset.setTimestamp(timestamp);
        RemotingCommand request = RemotingCommand.createResponseCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, null);
        request.setBody(searchOffset.encode());
        try {
            RemotingCommand response = this.nettyClient.invokeSync(addr, request, timeoutMillis);
            assert response != null;
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    if (response.getBody() != null) ;
                    SearchOffsetResponse searchOffsetResponse = SearchOffsetResponse.decode(response.getBody(), SearchOffsetResponse.class);

                    return searchOffsetResponse.getOffset();
                }
                default:
                    break;
            }
        } catch (Exception e) {
            logger.error("searchOffset error  {}", e);
        }
        logger.error("searchOffset fail {} {}", addr, topic);
        return 0;
    }


    void updateTopicRouteInfoFromNameServer(final String topic) {

        try {
            if (this.lockNamesrv.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData = getTopicRouteInfoFromNameServer(topic, timeoutMillis);

                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        boolean changed = isTopicRouteChange(old, topicRouteData);


                        if (changed) {
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneOne();

                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // 更新发布路由信息
                            {

                            }

                            // 更新订阅路由信息
                            {

                            }
                            logger.info("topicRouteTable.put TopicRouteData[{}]", cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);

                        }
                    } else {
                        logger.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (Exception e) {

                    logger.warn("updateTopicRouteInfoFromNameServer Exception", e);

                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                logger.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LockTimeoutMillis);
            }
        } catch (InterruptedException e) {
            logger.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }


    }

    public String getBrokerAddrByName(String name) {
        HashMap<Long, String> broker = this.brokerAddrTable.get(name);
        if (broker == null) {
            return null;
        }
        String addr = broker.get(All.MASTER_ID);
        return addr;
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {

        if (Strings.isNullOrEmpty(topic)) {
            return null;
        }
        RemotingCommand request = RemotingCommand.createResponseCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, null);
        request.setBody(JsonSerializable.encode(topic));


        RemotingCommand response = this.nettyClient.invokeSync(null, request, timeoutMillis);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {

                logger.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }


    public boolean isTopicRouteChange(TopicRouteData olddata, TopicRouteData nowdata) {
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

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {

        try {
            TopicRouteData topicRouteData = getTopicRouteInfoFromNameServer(topic, timeoutMillis);
            if (topicRouteData != null) {
                Set<MessageQueue> mqList = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                if (!mqList.isEmpty()) {
                    return mqList;
                } else {
                    return mqList;
                }
            }
        } catch (Exception e) {
            throw new MQClientException(
                    "Can not find Message Queue for this topic, " + topic + e);
        }

        return null;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {

            for (int i = 0; i < qd.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                mqList.add(mq);
            }

        }

        return mqList;
    }

    public void createTopic(final String addr, final String defaultTopic, final TopicInfo topicConfig, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        if (topicConfig == null) return;

        RemotingCommand request = RemotingCommand.createResponseCommand(RequestCode.CREATE_TOPIC, null);
        request.setBody(topicConfig.encode());

        RemotingCommand response = nettyClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

}
