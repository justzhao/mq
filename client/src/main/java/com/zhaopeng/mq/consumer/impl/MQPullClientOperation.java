package com.zhaopeng.mq.consumer.impl;

import com.google.common.base.Strings;
import com.zhaopeng.common.All;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.message.MessageInfo;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.query.QueryResult;
import com.zhaopeng.common.protocol.RequestCode;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.SearchOffsetRequest;
import com.zhaopeng.common.protocol.body.SearchOffsetResponse;
import com.zhaopeng.common.protocol.route.BrokerData;
import com.zhaopeng.common.protocol.route.TopicRouteData;
import com.zhaopeng.mq.consumer.AbstractMQClientOperation;
import com.zhaopeng.mq.consumer.MessageQueueListener;
import com.zhaopeng.mq.consumer.PullCallback;
import com.zhaopeng.mq.consumer.PullResult;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.protocol.JsonSerializable;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhaopeng on 2017/4/30.
 */
public class MQPullClientOperation extends AbstractMQClientOperation {

    private static final Logger logger = LoggerFactory.getLogger(MQPullClientOperation.class);

    private final long bootTime = System.currentTimeMillis();

    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();

    private long timeoutMillis = 6000;
    private long LockTimeoutMillis = 3000;

    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQPullClientOperationThread");
        }
    });


    public MQPullClientOperation(NettyClient nettyClient) {
        super(nettyClient);
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

        try {
            TopicRouteData topicRouteData = getTopicRouteInfoFromNameServer(key, timeoutMillis);
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
                                this.createTopic(addr, key, topicInfo, timeoutMillis);
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

        String brokerAddr = getBrokerAddrByName(mq.getBrokerName());
        if (null == brokerAddr) {
            // 重新根据topic获取addr

            updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = getBrokerAddrByName(mq.getBrokerName());

        }

        if (brokerAddr != null) {
            try {
                return searchOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timestamp,
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
    public void registerMessageQueueListener(String topic, MessageQueueListener listener) {

    }

    @Override
    public void sendMessageBack(MessageInfo msg, int delayLevel, String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return null;
    }


    public void createTopic(final String addr, final String defaultTopic, final TopicInfo topicInfo, final long timeoutMillis) {

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


    public String getBrokerAddrByName(String name) {
        HashMap<Long, String> broker = this.brokerAddrTable.get(name);
        if (broker == null) {
            return null;
        }
        String addr = broker.get(All.MASTER_ID);
        return addr;
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

    public  boolean isTopicRouteChange(TopicRouteData olddata, TopicRouteData nowdata){
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

}
