package com.zhaopeng.common.protocol.body;

import com.zhaopeng.remoting.protocol.JsonSerializable;

import java.io.Serializable;

/**
 * Created by zhaopeng on 2017/5/2.
 */
public class SearchOffsetRequest extends JsonSerializable implements Serializable {
    private static final long serialVersionUID = -1694891913683436367L;


    private String topic;

    private Integer queueId;

    private Long timestamp;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
