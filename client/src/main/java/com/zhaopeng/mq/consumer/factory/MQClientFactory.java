package com.zhaopeng.mq.consumer.factory;

import com.zhaopeng.common.protocol.route.TopicRouteData;
import com.zhaopeng.mq.ClientConfig;
import com.zhaopeng.remoting.netty.NettyServerConfig;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhaopeng on 2017/4/26.
 */
public class MQClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(MQClientFactory.class);

    private final ClientConfig clientConfig;

    private final String clientId;

    private final long bootTime = System.currentTimeMillis();

    private final NettyServerConfig nettyServerConfig;

    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();

    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<String, HashMap<Long, String>>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });


    public MQClientFactory(ClientConfig clientConfig, String clientId, NettyServerConfig nettyServerConfig) {
        this.clientConfig = clientConfig;
        this.clientId = clientId;
        this.nettyServerConfig = nettyServerConfig;






        logger.info("created a new client Instance, ClinetID:  {} {}, serializeType={}", //

                this.clientId, //
                this.clientConfig, //
                 RemotingCommand.getSerializeType());
    }


}
