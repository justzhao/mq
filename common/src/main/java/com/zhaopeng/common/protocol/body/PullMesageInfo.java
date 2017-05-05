package com.zhaopeng.common.protocol.body;

import com.zhaopeng.remoting.protocol.JsonSerializable;

/**
 * Created by zhaopeng on 2017/5/5.
 */
public class PullMesageInfo extends JsonSerializable {

    private String topic;

    private Integer queueId;

    private Long queueOffset;

    private Integer maxMsgNums;

    private Long commitOffset;

    public PullMesageInfo(String topic, Integer queueId, Long queueOffset, Integer maxMsgNums, Long commitOffset) {
        this.topic = topic;
        this.queueId = queueId;
        this.queueOffset = queueOffset;
        this.maxMsgNums = maxMsgNums;
        this.commitOffset = commitOffset;
    }

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

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public Integer getMaxMsgNums() {
        return maxMsgNums;
    }

    public void setMaxMsgNums(Integer maxMsgNums) {
        this.maxMsgNums = maxMsgNums;
    }

    public Long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }
}
