package com.zhaopeng.mq;

import com.google.common.base.Strings;
import com.zhaopeng.common.All;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.enums.PullStatus;
import com.zhaopeng.common.client.enums.SendStatus;
import com.zhaopeng.common.client.message.*;
import com.zhaopeng.common.message.MessageDecoder;
import com.zhaopeng.common.message.PullMessage;
import com.zhaopeng.common.protocol.RequestCode;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.common.protocol.body.SearchOffsetRequest;
import com.zhaopeng.common.protocol.body.SearchOffsetResponse;
import com.zhaopeng.common.protocol.route.BrokerData;
import com.zhaopeng.common.protocol.route.QueueData;
import com.zhaopeng.common.protocol.route.TopicRouteData;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;
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
public class MQAdminClientAPIImpl implements MQAdminClientAPI {
    private static final Logger logger = LoggerFactory.getLogger(MQAdminClientAPIImpl.class);


    private String namesrv;

    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();

    private long timeoutMillis = 6000;
    private long LockTimeoutMillis = 3000;

    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    protected final NettyClient nettyClient;

    public MQAdminClientAPIImpl(NettyClient nettyClient, String addr) {
        this.nettyClient = nettyClient;
        this.namesrv = addr;
    }

    public long searchOffset(final String addr, final String topic, final int queueId, final long timestamp, final long timeoutMillis) {
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


    public void updateTopicRouteInfoFromNameServer(String topic) {

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
        return broker.get(All.MASTER_ID);

    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {

        if (Strings.isNullOrEmpty(topic)) {
            return null;
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, null);
        request.setBody(topic.getBytes(JsonSerializable.CHARSET_UTF8));
        RemotingCommand response = this.nettyClient.invokeSync(namesrv, request, timeoutMillis);

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
        return null;
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
            logger.error("Can not find Message Queue for this topic, {} {}", topic, e);

        }

        return null;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            // 针对每个队列，每个队列可以设置nums个读队列。
            for (int i = 0; i < qd.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                mqList.add(mq);
            }

        }

        return mqList;
    }


    public void createTopic(final String topic, final TopicInfo topicConfig, final long timeoutMillis) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        createTopic(namesrv, topic, topicConfig, timeoutMillis);

    }

    public static void main(String args[]) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        String topic = "zhaopeng";

        TopicInfo topicInfo = new TopicInfo(topic);
        topicInfo.setReadQueueNums(1);
        topicInfo.setWriteQueueNums(1);

        NettyClientConfig clientConfig = new NettyClientConfig();

        NettyClient nettyClient = new NettyClient(clientConfig);
        String addr = "127.0.0.1";

        MQAdminClientAPIImpl mqAdminClientAPI = new MQAdminClientAPIImpl(nettyClient, addr);
        mqAdminClientAPI.createTopic(addr, topicInfo, 3 * 1000);

    }

    public void createTopic(final String addr, final String defaultTopic, final TopicInfo topicConfig, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        if (topicConfig == null) return;

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CREATE_TOPIC, null);
        topicConfig.setTopicName(defaultTopic);
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

    public PullResult pull(MessageQueue mq, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String topic = mq.getTopic();

        String brokerAddr = getBrokerAddrByName(mq.getBrokerName());
        if (Strings.isNullOrEmpty(brokerAddr)) {
            updateTopicRouteInfoFromNameServer(topic);
        }
        brokerAddr = getBrokerAddrByName(mq.getBrokerName());
        if (Strings.isNullOrEmpty(brokerAddr)) {

            logger.error("Can not find Broker Address for this topic  {}", topic);
            return null;
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        PullMesageInfo pullMesageInfo = new PullMesageInfo(topic, mq.getQueueId(), offset, maxNums, 0l);
        request.setBody(pullMesageInfo.encode());
        RemotingCommand respone = this.nettyClient.invokeSync(brokerAddr, request, timeoutMillis);
        if (respone.getCode() == ResponseCode.SUCCESS) {
            if (respone.getBody() != null) {
                // 这反序列化数据有问题. 二进制数据反序列化为对象.
                List<PullMessage> msgs = MessageDecoder.decodes(respone.getBody());

                PullResult result = new PullResult();
                result.setMsgs(msgs);
                result.setPullStatus(PullStatus.FOUND);
                result.setMaxOffset(respone.getMaxOffset());
                result.setMinOffset(respone.getMinOffset());
                result.setNextBeginOffset(respone.getMinOffset());

                return result;

            }
        } else if (respone.getCode() == ResponseCode.FAIL) {

            PullResult result = new PullResult(PullStatus.FAIL);

            return result;

        }

        return null;
    }

    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        if (Strings.isNullOrEmpty(topic)) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, null);

        request.setBody(topic.getBytes(JsonSerializable.CHARSET_UTF8));
        RemotingCommand response = nettyClient.invokeSync(namesrv, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.FAIL: {

                return null;
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

    public SendResult send(String brokerAddr, final MessageQueue mq, Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        if (Strings.isNullOrEmpty(brokerAddr)) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);

        SendMessage sendMessage = new SendMessage();
        sendMessage.setBrokerName(mq.getBrokerName());
        sendMessage.setTopic(mq.getTopic());
        sendMessage.setBrokerAddr(brokerAddr);
        sendMessage.setMsg(msg);
        sendMessage.setQueueId(mq.getQueueId());
        request.setBody(sendMessage.encode());
        RemotingCommand response = this.nettyClient.invokeSync(brokerAddr, request, timeout);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    SendResult result = JsonSerializable.decode(response.getBody(), SendResult.class);
                    result.setSendStatus(SendStatus.OK);
                    return result;
                }
            }
            case ResponseCode.FAIL: {
                SendResult result = new SendResult();
                result.setSendStatus(SendStatus.NOTOK);
                return result;
            }
            default: {
                return null;
            }
        }

    }


}
