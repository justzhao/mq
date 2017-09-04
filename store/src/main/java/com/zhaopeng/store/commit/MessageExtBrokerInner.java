package com.zhaopeng.store.commit;

import java.util.Map;

/**
 * Created by zhaopeng on 2017/9/4.
 */
public class MessageExtBrokerInner {

    private String topic;
    private String brokerName;
    private String brokerAddr;
    private int queueOffset;
    private int queueId;

    private String bornhost;


    private int flag;
    private Map<String, String> properties;
    private byte[] body;

    private String msgId;

    private long commitLogOffset;


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

    public int getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(int queueOffset) {
        this.queueOffset = queueOffset;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getBornhost() {
        return bornhost;
    }

    public void setBornhost(String bornhost) {
        this.bornhost = bornhost;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }
}
