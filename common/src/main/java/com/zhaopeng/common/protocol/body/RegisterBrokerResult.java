package com.zhaopeng.common.protocol.body;

import com.zhaopeng.remoting.protocol.JsonSerializable;

/**
 * Created by zhaopeng on 2017/4/13.
 */
public class RegisterBrokerResult extends JsonSerializable {

    private String serverAddr;

    public String getServerAddr() {
        return serverAddr;
    }

    public void setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;
    }
}
