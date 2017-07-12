package com.zhaopeng.mq.config;

import com.zhaopeng.common.All;

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




    private String brokerName = "default";

    private Long brokerId = All.MASTER_ID;

    private int registerBrokerTimeoutMills = 500;

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    public int getRegisterBrokerTimeoutMills() {
        return registerBrokerTimeoutMills;
    }

    public void setRegisterBrokerTimeoutMills(int registerBrokerTimeoutMills) {
        this.registerBrokerTimeoutMills = registerBrokerTimeoutMills;
    }
}
