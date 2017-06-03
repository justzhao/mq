package com.zhaopeng.mq;

import com.zhaopeng.common.ThreadFactoryImpl;
import com.zhaopeng.mq.config.BrokerConfig;
import com.zhaopeng.mq.processor.BrokerProcessor;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;
import com.zhaopeng.remoting.netty.NettyServer;
import com.zhaopeng.remoting.netty.NettyServerConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhaopeng on 2017/4/22.
 */
public class BrokerController {

    private NettyServer nettyServer;

    private NettyClient nettyClient;


    private BrokerConfig brokerConfig;

    private NettyClientConfig nettyClientConfig;

    private NettyServerConfig nettyServerConfig;

    private ExecutorService executorService;

    public BrokerController() {
        nettyClient = new NettyClient(nettyClientConfig);
        nettyServer = new NettyServer(nettyServerConfig, null);
        executorService = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("BrokerControllerThead_"));
    }

    public void start() {

        nettyServer.registerDefaultProcessor(new BrokerProcessor(), executorService);
        nettyClient.start();
        nettyServer.start();


    }


}
