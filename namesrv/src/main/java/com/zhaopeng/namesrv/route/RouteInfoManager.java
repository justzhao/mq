package com.zhaopeng.namesrv.route;

import com.zhaopeng.common.DataVersion;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerResult;
import com.zhaopeng.common.protocol.route.BrokerData;
import com.zhaopeng.common.protocol.route.QueueData;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by zhaopeng on 2017/4/9.
 */
public class RouteInfoManager {

    private static final Logger logger = LoggerFactory.getLogger(RouteInfoManager.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<>();
        this.brokerAddrTable = new HashMap<>();
        this.brokerLiveTable = new HashMap<>();
    }

    /**
     * 关闭channel
     *
     * @param remoteAddr
     * @param channel
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {

    }

    public void scanAliveBroker() {

    }

    public void printAllConfPeriodically() {

    }

    public RegisterBrokerResult registerBroker(RegisterBrokerInfo brokerInfo, final Channel channel) {

        try {

            lock.writeLock().lockInterruptibly();

            String brokerAddr = brokerInfo.getServerAddr();
            String brokerName = brokerInfo.getBrokerName();
            boolean registerFirst = false;

            BrokerData brokerData = this.brokerAddrTable.get(brokerAddr);

            if (null == brokerData) {
                registerFirst = true;
                brokerData = new BrokerData();
                brokerData.setBrokerName(brokerName);
                this.brokerAddrTable.put(brokerName, brokerData);
            }
            String oldAddr = brokerData.getBrokerAddrs().put(brokerInfo.getBrokerId(), brokerInfo.getServerAddr());
            registerFirst = registerFirst || (null == oldAddr);

            // 存放topic 队列信息
            if (registerFirst) {
                ConcurrentHashMap<String, TopicInfo> topicInfo = brokerInfo.getTopicConfigTable();
                if (null != topicInfo) {
                    for (Map.Entry<String, TopicInfo> entry : topicInfo.entrySet()) {
                        this.createAndUpdateQueueData(brokerName, entry.getValue());
                    }

                }
            }


            // 存放brokerLive信息
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerInfo.getServerAddr(),
                    new BrokerLiveInfo(System.currentTimeMillis(), brokerInfo.getDataVersion(), channel));
            if (null == prevBrokerLiveInfo) {
                logger.info("new broker registerd, {} ", brokerAddr);
            }


        } catch (InterruptedException e) {
            logger.error("registerBroker Exception", e);
        } finally {
            lock.writeLock().unlock();
        }

        RegisterBrokerResult result = new RegisterBrokerResult();

        result.setServerAddr(brokerInfo.getServerAddr());
        return result;
    }

    /**
     * 新增或者更新一个topic
     *
     * @param brokerName
     * @param topicInfo
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicInfo topicInfo) {

        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setReadQueueNums(topicInfo.getReadQueueNums());
        queueData.setWriteQueueNums(topicInfo.getWriteQueueNums());

        List<QueueData> queueDatas = this.topicQueueTable.get(topicInfo.getTopicName());

        if (queueData == null) {
            queueDatas = new ArrayList<>();
            queueDatas.add(queueData);
            this.topicQueueTable.put(topicInfo.getTopicName(), queueDatas);
            logger.info("new topic registerd, {} {}", topicInfo.getTopicName(), queueData);

        } else {

            boolean addNew = true;
            Iterator<QueueData> itor = queueDatas.iterator();
            while (itor.hasNext()) {
                QueueData q = itor.next();

                if (brokerName.equals(q.getBrokerName())) {

                    if (queueData.equals(q)) {
                        addNew = false;
                    } else {
                        logger.info("topic changed, {} OLD: {} NEW: {}", topicInfo.getTopicName(), q,
                                queueData);
                        itor.remove();
                    }


                }

            }
            if (addNew) {
                queueDatas.add(queueData);
            }
        }

    }


}

class BrokerLiveInfo {
    private long lastUpdateTimestamp;
    private DataVersion dataVersion;
    private Channel channel;

    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
    }
}