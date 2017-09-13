package com.zhaopeng.common.message;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by zhaopeng on 2017/9/12.
 */
public class PullMessage implements Serializable {

    private static final long serialVersionUID = -8133720667570241771L;

    private int size;



    private int  crc;

    private int queueId;

    private Long queueOffset;

    private Long phyOffset;

    private Long storeTime;

    private int bodyLength;

    private byte[] body;

    private String topic;



    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }


    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public Long getPhyOffset() {
        return phyOffset;
    }

    public void setPhyOffset(Long phyOffset) {
        this.phyOffset = phyOffset;
    }

    public Long getStoreTime() {
        return storeTime;
    }

    public void setStoreTime(Long storeTime) {
        this.storeTime = storeTime;
    }

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "PullMessage{" +
                "size=" + size +
                ", crc=" + crc +
                ", queueId=" + queueId +
                ", queueOffset=" + queueOffset +
                ", phyOffset=" + phyOffset +
                ", storeTime=" + storeTime +
                ", bodyLength=" + bodyLength +
                ", body=" + Arrays.toString(body) +
                ", topic='" + topic + '\'' +
                '}';
    }
}
