package com.zhaopeng.mq;

import com.zhaopeng.common.ThreadFactoryImpl;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerResult;
import com.zhaopeng.mq.config.BrokerConfig;
import com.zhaopeng.mq.processor.BrokerClientProcessor;
import com.zhaopeng.mq.processor.BrokerServerProcessor;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;
import com.zhaopeng.remoting.netty.NettyServer;
import com.zhaopeng.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaopeng on 2017/4/22.
 */
public class BrokerController {


    private static final Logger logger = LoggerFactory.getLogger(BrokerController.class);

    private NettyServer nettyServer;

    private NettyClient nettyClient;


    private BrokerConfig brokerConfig;

    private NettyClientConfig nettyClientConfig;

    private NettyServerConfig nettyServerConfig;

    private ExecutorService serverExecutor;

    private ExecutorService clientExecutor;

    private BrokerOutApi brokerOutApi;

    private BrokerServerProcessor brokerServerProcessor;

    private BrokerClientProcessor brokerClientProcessor;

    private String namesrv;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerControllerScheduledThread"));

    public BrokerController(String namesrv) {
        nettyClientConfig = new NettyClientConfig();

        nettyServerConfig = new NettyServerConfig();

        // broker Server端口
        nettyServerConfig.setPort(9999);

        brokerConfig = new BrokerConfig();
        nettyClient = new NettyClient(nettyClientConfig);
        nettyServer = new NettyServer(nettyServerConfig, null);
        serverExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new
                ThreadFactoryImpl("ServerExecutor_"));
        clientExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new
                ThreadFactoryImpl("ClientExecutor_"));
        brokerOutApi = new BrokerOutApi(nettyClient);
        this.namesrv = namesrv;
        brokerOutApi.updateNamesrv(namesrv);
        brokerServerProcessor = new BrokerServerProcessor();
        brokerClientProcessor = new BrokerClientProcessor();
    }

    public void start() {

        nettyServer.registerDefaultProcessor(brokerServerProcessor, serverExecutor);
        nettyClient.registerDefaultProcessor(brokerClientProcessor, clientExecutor);
        nettyClient.start();
        nettyServer.start();

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBroker();
                } catch (Throwable e) {
                    logger.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);


    }

    public synchronized void registerBroker() {


        List<TopicInfo> topicInfos = brokerServerProcessor.getTopicInfosFromConsumeQueue();
        brokerOutApi.registerTopicInfos(topicInfos,this.brokerConfig.getRegisterBrokerTimeoutMills());


        RegisterBrokerResult registerBrokerResult = this.brokerOutApi.registerBrokerAll(
                this.brokerConfig.getBrokerName(),
                this.getBrokerAddr(), //
                this.brokerConfig.getBrokerName(), //
                this.brokerConfig.getBrokerId(), //
                false,// 需要返回值
                this.brokerConfig.getRegisterBrokerTimeoutMills());

        logger.info(" register broker Result is {}", registerBrokerResult);

    }

    private String getBrokerAddr() {

        try {
            return InetAddress.getLocalHost().getHostAddress() + ":" + nettyServerConfig.getPort();
        } catch (UnknownHostException e) {
            e.printStackTrace();

        }

        return "127.0.0.1:9999";
    }

}
