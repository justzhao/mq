package com.zhaopeng.namesrv.route;

import com.zhaopeng.common.DataVersion;
import com.zhaopeng.common.protocol.body.RegisterBrokerInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerResult;
import com.zhaopeng.common.protocol.route.BrokerData;
import com.zhaopeng.common.protocol.route.QueueData;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
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

    public RegisterBrokerResult registerBroker(RegisterBrokerInfo brokerInfo) {

        try {

            lock.writeLock().lockInterruptibly();

        } catch (InterruptedException e) {
            logger.error("registerBroker Exception", e);
        } finally {
            lock.writeLock().unlock();
        }

        RegisterBrokerResult result = new RegisterBrokerResult();

        result.setServerAddr(brokerInfo.getServerAddr());
        return result;
    }


}

class BrokerLiveInfo {
    private long lastUpdateTimestamp;
    private DataVersion dataVersion;
    private Channel channel;


}