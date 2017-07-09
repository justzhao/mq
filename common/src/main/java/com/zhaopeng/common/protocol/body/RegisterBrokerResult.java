package com.zhaopeng.common.protocol.body;

import com.zhaopeng.remoting.protocol.JsonSerializable;

import java.io.Serializable;

/**
 * Created by zhaopeng on 2017/4/13.
 */
public class RegisterBrokerResult extends JsonSerializable implements Serializable {


    private static final long serialVersionUID = 7764786267696690045L;
    private String serverAddr;

    public String getServerAddr() {
        return serverAddr;
    }

    public void setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;
    }

    @Override
    public String toString() {
        return "RegisterBrokerResult{" +
                "serverAddr='" + serverAddr + '\'' +
                '}';
    }
}
