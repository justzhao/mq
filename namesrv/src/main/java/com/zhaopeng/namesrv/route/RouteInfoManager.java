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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     * @param brokerName
     * @param topicInfo
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicInfo topicInfo) {

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