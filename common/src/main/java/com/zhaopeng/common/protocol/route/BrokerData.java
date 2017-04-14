package com.zhaopeng.common.protocol.route;

import java.util.HashMap;

/**
 * Created by zhaopeng on 2017/4/13.
 */
public class BrokerData {

    private String brokerName;
    private HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs=new HashMap<>();

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public HashMap<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(HashMap<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }
}
