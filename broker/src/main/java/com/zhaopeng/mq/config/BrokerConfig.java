package com.zhaopeng.mq.config;

/**
 * Created by zhaopeng on 2017/5/19.
 */
public class BrokerConfig {

    private int clientWorkerThreads = 4;
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private int clientOnewaySemaphoreValue = 100;
    private int clientAsyncSemaphoreValue = 100;
    private int connectTimeoutMillis = 3000;
    private long channelNotActiveInterval = 1000 * 60;
    public int socketSndbufSize = 65535;

    public int socketRcvbufSize = 65535;
}
