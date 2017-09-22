package com.zhaopeng.namesrv;

import com.zhaopeng.common.ThreadFactoryImpl;
import com.zhaopeng.common.namesrv.NameSrvConfig;
import com.zhaopeng.namesrv.listener.BrokerStatusListener;
import com.zhaopeng.namesrv.processor.DefaultRequestProcessor;
import com.zhaopeng.namesrv.route.RouteInfoManager;
import com.zhaopeng.remoting.netty.NettyServer;
import com.zhaopeng.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaopeng on 2017/4/5.
 */
public class NameSrvController {

    private static final Logger log = LoggerFactory.getLogger(NameSrvController.class);

    private final NameSrvConfig nameSrvConfig;

    private final NettyServerConfig nettyServerConfig;


    private final RouteInfoManager routeInfoManager;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "NSScheduledThread"));


    private ExecutorService executorService;

    private BrokerStatusListener brokerStatusListener;

    private NettyServer nettyServer;

    public NameSrvController(NameSrvConfig nameSrvConfig, NettyServerConfig nettyServerConfig) {
        this.nameSrvConfig = nameSrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.brokerStatusListener = new BrokerStatusListener(this);
        this.routeInfoManager = new RouteInfoManager();
    }


    public NameSrvConfig getNameSrvConfig() {
        return nameSrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public boolean init() {
        // netty
        this.nettyServer = new NettyServer(nettyServerConfig, brokerStatusListener);
        // 线程池
        this.executorService =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        // 注册请求处理器
        this.registerProcessor();
        // 定时任务
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                NameSrvController.this.routeInfoManager.scanAliveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                NameSrvController.this.routeInfoManager.printAllNameSrvStatistics();
            }
        }, 1, 10, TimeUnit.SECONDS);


        return true;
    }

    public boolean start() {

        nettyServer.start();
        return true;
    }


    private void registerProcessor() {
        this.nettyServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.executorService);
    }

    public void shutdown() {
        nettyServer.shutdown();
        executorService.shutdown();
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public BrokerStatusListener getBrokerStatusListener() {
        return brokerStatusListener;
    }

    public void setBrokerStatusListener(BrokerStatusListener brokerStatusListener) {
        this.brokerStatusListener = brokerStatusListener;
    }

    public NettyServer getNettyServer() {
        return nettyServer;
    }

    public void setNettyServer(NettyServer nettyServer) {
        this.nettyServer = nettyServer;
    }
}
