package com.zhaopeng.common.client.message;

import com.zhaopeng.remoting.protocol.JsonSerializable;

/**
 * Created by zhaopeng on 2017/5/13.
 */
public class SendMessage extends JsonSerializable{
    private String topic;
    private String brokerName;
    private String brokerAddr;


    private Message msg;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public Message getMsg() {
        return msg;
    }

    public void setMsg(Message msg) {
        this.msg = msg;
    }
}
