package com.zhaopeng.common.namesrv;

import com.zhaopeng.common.All;

/**
 * Created by zhaopeng on 2017/4/6.
 */
public class NameSrvConfig {

    private String mqHome = System.getProperty(All.MQ_HOME_ENV, System.getenv(All.MQ_HOME_ENV));


    private  boolean orderMessage=false;

    private boolean cluster=false;

    public String getMqHome() {
        return mqHome;
    }

    public void setMqHome(String mqHome) {
        this.mqHome = mqHome;
    }

    public boolean isOrderMessage() {
        return orderMessage;
    }

    public void setOrderMessage(boolean orderMessage) {
        this.orderMessage = orderMessage;
    }

    public boolean isCluster() {
        return cluster;
    }

    public void setCluster(boolean cluster) {
        this.cluster = cluster;
    }
}
