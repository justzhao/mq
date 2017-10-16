package com.zhaopeng.common.protocol.route;

import com.zhaopeng.common.All;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Created by zhaopeng on 2017/4/13.
 */
public class BrokerData  implements Comparable<BrokerData>{

    private String brokerName;
    private HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs=new HashMap<>();

    private final Random random = new Random();

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

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String selectBrokerAddr() {
        String addr = this.brokerAddrs.get(All.MASTER_ID);

        if (addr == null) {
            List<String> addrs = new ArrayList<String>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }

        return addr;
    }

    @Override
    public String toString() {
        return "BrokerData{" +
                "brokerName='" + brokerName + '\'' +
                ", brokerAddrs=" + brokerAddrs +
                '}';
    }
}
