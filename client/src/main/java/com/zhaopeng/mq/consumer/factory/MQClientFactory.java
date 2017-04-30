package com.zhaopeng.mq.consumer.factory;

import com.zhaopeng.common.protocol.RequestCode;
import com.zhaopeng.common.protocol.route.TopicRouteData;
import com.zhaopeng.mq.ClientConfig;
import com.zhaopeng.mq.consumer.MQPullConsumer;
import com.zhaopeng.mq.consumer.impl.ClientRemotingProcessor;
import com.zhaopeng.mq.consumer.impl.DefaultMQPullConsumer;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;
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

    private final NettyClientConfig nettyClientConfig;

    private final ClientRemotingProcessor clientRemotingProcessor;

    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();

    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });

    private final MQPullConsumer mqPullConsumer;


    public MQClientFactory(ClientConfig clientConfig, String clientId, NettyClientConfig nettyClientConfig) {
        this.clientConfig = clientConfig;
        this.clientId = clientId;
        this.nettyClientConfig = nettyClientConfig;
        this.clientRemotingProcessor=new ClientRemotingProcessor();

        NettyClient nettyClient=new NettyClient(nettyClientConfig);



        nettyClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor, null);

        nettyClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor, null);

        nettyClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this.clientRemotingProcessor, null);

        nettyClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);

        nettyClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);

        this.mqPullConsumer=new DefaultMQPullConsumer(nettyClient,clientRemotingProcessor);
        logger.info("created a new client Instance, ClinetID:  {} {}, serializeType={}", //
                this.clientId, //
                this.clientConfig, //
                RemotingCommand.getSerializeType());
    }


}
