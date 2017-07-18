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

    public PullMesageInfo() {
    }

    /**
     *每一个消息队列里面还有偏移(commitOffset, offset)的区别，为什么有2个offset
     * offset 当前MessageQueue消费进度的偏移量
     * commitOffset . 确认已经被消费的消息偏移量。也就是真正消息消费的进度
     */

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
