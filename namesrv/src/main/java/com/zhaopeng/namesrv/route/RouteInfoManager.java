package com.zhaopeng.namesrv.route;

import com.google.common.base.Strings;
import com.zhaopeng.common.DataVersion;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.protocol.body.CreateTopicResult;
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

    // broker的默认失效时间,2分钟内没更新
    private final static long BrokerChannelExpiredTime = 1000 * 60 * 2;

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

        //   initData();
    }

    // 仅仅是为来初始化有数据
    private void initData() {
        TopicInfo topicInfo = new TopicInfo("default");
        RegisterBrokerInfo brokerInfo = new RegisterBrokerInfo();
        DataVersion v = new DataVersion();

        brokerInfo.setDataVersion(v);
        brokerInfo.setBrokerId(1l);
        brokerInfo.setServerAddr("127.0.0.1");
        brokerInfo.setBrokerName("default");
        ConcurrentHashMap<String, TopicInfo> map = new ConcurrentHashMap<>();
        map.put("default", topicInfo);
        brokerInfo.setTopicConfigTable(map);

        Channel channel = null;
        registerBroker(brokerInfo, channel);
    }


    public CreateTopicResult createTopic(TopicInfo topicInfo) {
        CreateTopicResult result = new CreateTopicResult();
        if (topicInfo == null) {
            return result;
        }
        List<QueueData> queueDatas = this.topicQueueTable.get(topicInfo.getTopicName());
        if (queueDatas == null) {
            queueDatas = new ArrayList<>();
        }
        QueueData queueData = new QueueData();
        queueData.setBrokerName("default");
        queueData.setWriteQueueNums(topicInfo.getWriteQueueNums());
        queueData.setReadQueueNums(topicInfo.getReadQueueNums());
        queueDatas.add(queueData);
        this.topicQueueTable.put(topicInfo.getTopicName(), queueDatas);
        logger.info("new topic registerd, {} {}", topicInfo.getTopicName(), queueData);

        return result;
    }

    /**
     * 关闭channel
     *
     * @param remoteAddr
     * @param channel
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        // 根据要被关闭的Channel找到对应的broker的地址
        String brokerAddrFound = null;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    Iterator<Map.Entry<String, BrokerLiveInfo>> itBrokerLiveTable =
                            this.brokerLiveTable.entrySet().iterator();
                    while (itBrokerLiveTable.hasNext()) {
                        Map.Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                logger.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            logger.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        // 如果真的有broker需要被关闭
        if (Strings.isNullOrEmpty(brokerAddrFound)) {

            try {
                try {
                    this.lock.writeLock().lockInterruptibly();
                    this.brokerLiveTable.remove(brokerAddrFound);

                    String brokerNameFound = null;
                    boolean removeBrokerName = false;
                    Iterator<Map.Entry<String, BrokerData>> itBrokerAddrTable =
                            this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();

                        Iterator<Map.Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry<Long, String> entry = it.next();
                            Long brokerId = entry.getKey();
                            String brokerAddr = entry.getValue();
                            if (brokerAddr.equals(brokerAddrFound)) {
                                brokerNameFound = brokerData.getBrokerName();
                                it.remove();
                                logger.info(
                                        "remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                                        brokerId, brokerAddr);
                                break;
                            }
                        }

                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            logger.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                                    brokerData.getBrokerName());
                        }
                    }


                    if (removeBrokerName) {
                        Iterator<Map.Entry<String, List<QueueData>>> itTopicQueueTable =
                                this.topicQueueTable.entrySet().iterator();
                        while (itTopicQueueTable.hasNext()) {
                            Map.Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                            String topic = entry.getKey();
                            List<QueueData> queueDataList = entry.getValue();

                            Iterator<QueueData> itQueueData = queueDataList.iterator();
                            while (itQueueData.hasNext()) {
                                QueueData queueData = itQueueData.next();
                                if (queueData.getBrokerName().equals(brokerNameFound)) {
                                    itQueueData.remove();
                                    logger.info(
                                            "remove topic[{} {}], from topicQueueTable, because channel destroyed",
                                            topic, queueData);
                                }
                            }

                            if (queueDataList.isEmpty()) {
                                itTopicQueueTable.remove();
                                logger.info(
                                        "remove topic[{}] all queue, from topicQueueTable, because channel destroyed",
                                        topic);
                            }
                        }
                    }
                } finally {
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                logger.error("onChannelDestroy Exception", e);
            }
        }


    }

    public void scanAliveBroker() {
        Iterator<Map.Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            // 分钟内部更新就认为失效
            if ((last + BrokerChannelExpiredTime) < System.currentTimeMillis()) {
                next.getValue().getChannel().close();
                it.remove();
                logger.info("The broker channel expired, {} {}ms", next.getKey(), BrokerChannelExpiredTime);
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
            }
        }


    }

    public void printAllNameSrvStatistics() {

        try {
            try {
                this.lock.readLock().lockInterruptibly();
                logger.info("--------------------------------------------------------");

                // 打印topic信息
                logger.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                Iterator<Map.Entry<String, List<QueueData>>> itTopic = this.topicQueueTable.entrySet().iterator();
                while (itTopic.hasNext()) {
                    Map.Entry<String, List<QueueData>> next = itTopic.next();
                    logger.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                }

                // 根据brokerName打印broker的信息
                logger.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                Iterator<Map.Entry<String, BrokerData>> itName = this.brokerAddrTable.entrySet().iterator();
                while (itName.hasNext()) {
                    Map.Entry<String, BrokerData> next = itName.next();
                    logger.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                }

                // 根据brokerAddress打印broker的信息
                logger.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                Iterator<Map.Entry<String, BrokerLiveInfo>> itAddr = this.brokerLiveTable.entrySet().iterator();
                while (itAddr.hasNext()) {
                    Map.Entry<String, BrokerLiveInfo> next = itAddr.next();
                    logger.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                }


            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            logger.error("printAllPeriodically Exception", e);
        }

    }

    public RegisterBrokerResult registerBroker(RegisterBrokerInfo brokerInfo, final Channel channel) {
        try {
            logger.info("registerBroker Info {}", brokerInfo);
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
        if (queueDatas == null) {
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
        return topicRouteData;
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


    /**
     * 删除一个topic信息
     *
     * @param topic
     */
    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            logger.error("deleteTopic Exception", e);
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

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo{" +
                "lastUpdateTimestamp=" + lastUpdateTimestamp +
                ", dataVersion=" + dataVersion +
                ", channel=" + channel +
                '}';
    }
}