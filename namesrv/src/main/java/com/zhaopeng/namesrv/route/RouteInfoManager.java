package com.zhaopeng.namesrv.route;

import com.zhaopeng.common.DataVersion;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerResult;
import com.zhaopeng.common.protocol.body.TopicList;
import com.zhaopeng.common.protocol.route.BrokerData;
import com.zhaopeng.common.protocol.route.QueueData;
import com.zhaopeng.common.protocol.route.TopicRouteData;
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
    // 可以有多个brokerName.用brokerId区分
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    // 只有一个brokerAddr
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

            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (null == brokerData) {
                registerFirst = true;
                brokerData = new BrokerData();
                brokerData.setBrokerName(brokerName);
                this.brokerAddrTable.put(brokerName, brokerData);
            }
            // 多个broker可能有相同的名字
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
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
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

    public void unRegisterBroker(RegisterBrokerInfo brokerInfo) {
        try {
            this.lock.writeLock().lockInterruptibly();
            String brokerAddr = brokerInfo.getServerAddr();
            String brokerName = brokerInfo.getBrokerName();
            // brokerLive
            BrokerLiveInfo rmL = this.brokerLiveTable.remove(brokerAddr);

            if (rmL != null) {
                logger.info("unregisterBroker, remove from brokerLiveTable {}", rmL);
            }
            //brokerAddr;
            BrokerData rmD = this.brokerAddrTable.get(brokerName);
            if (rmD != null) {
                String addrs = rmD.getBrokerAddrs().remove(brokerInfo.getBrokerId());
                logger.info("unregisterBroker, remove {} from brokerAddrTable {}", addrs, this.brokerAddrTable);
            }
            //topic
            this.removeTopicByBrokerName(brokerName);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            this.lock.writeLock().unlock();

        }
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
                        logger.info("topic changed, {} OLD: {} NEW: {}", topicInfo.getTopicName(), q, queueData);
                        itor.remove();
                    }
                }
            }
            if (addNew) {
                queueDatas.add(queueData);
            }
        }

    }

    /**
     * 删除一个broker中的topic信息
     *
     * @param brokerName
     */
    private void removeTopicByBrokerName(String brokerName) {

        Iterator<Map.Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Map.Entry<String, List<QueueData>> entry = itMap.next();
            String topic = entry.getKey();
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    logger.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
                    it.remove();
                }
            }

            if (queueDataList.isEmpty()) {
                logger.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
        }

    }

    /**
     * 根据 topic选择路由
     *
     * @param topic
     * @return
     */
    public TopicRouteData selectTopicRouteData(final String topic) {

        TopicRouteData topicRouteData = new TopicRouteData();
        //LinkedList 方便快速添数据
        List<BrokerData> brokerDatas = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDatas);

        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        try {
            this.lock.readLock().lockInterruptibly();

            List<QueueData> queueDatas = this.topicQueueTable.get(topic);
            if (queueDatas != null) {
                foundQueueData = true;
                topicRouteData.setQueueDatas(queueDatas);
                Set<String> brokerNames = new HashSet<>();
                for (QueueData qd : queueDatas) {
                    if (qd != null) {
                        brokerNames.add(qd.getBrokerName());
                    }
                }
                for (String name : brokerNames) {
                    BrokerData brokerData = this.brokerAddrTable.get(name);
                    if (brokerData != null) {
                        foundBrokerData = true;
                        BrokerData bd = new BrokerData();
                        bd.setBrokerName(brokerData.getBrokerName());
                        bd.setBrokerAddrs((HashMap<Long, String>) brokerData.getBrokerAddrs().clone());
                        brokerDatas.add(bd);

                    }
                }

            }

        } catch (InterruptedException e) {
            logger.error("selectTopicRouteData error {}", e);
        } finally {
            this.lock.readLock().unlock();
        }
        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }
        return null;
    }

    public TopicList getAllTopicList() {
        TopicList topicList = new TopicList();

        try {
            try {
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            logger.error("getAllTopicList Exception", e);
        }

        return topicList;
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