package com.zhaopeng.store.entity;

import com.zhaopeng.common.client.message.SendMessage;

/**
 * Created by zhaopeng on 2017/7/31.
 */
public class MessageExtBrokerInner extends SendMessage {

    private long bodyLength;

    private String host;

    public long getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(long bodyLength) {
        this.bodyLength = bodyLength;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
