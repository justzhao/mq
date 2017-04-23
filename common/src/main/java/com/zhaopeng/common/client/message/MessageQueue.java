package com.zhaopeng.common.client.message;

import java.io.Serializable;

/**
 * Created by zhaopeng on 2017/4/23.
 */
public class MessageQueue implements Comparable<MessageQueue>, Serializable {


    private static final long serialVersionUID = -8400599627821843041L;

    private String topic;
    private String brokerName;
    private int queueId;

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

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

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    @Override
    public int compareTo(MessageQueue o) {
        return 0;
    }
}
