package com.zhaopeng.store.entity;

/**
 * Created by zhaopeng on 2017/8/25.
 */
public class QueueRequest {

    private final String topic;
    private final int queueId;
    private final long commitLogOffset;
    private final int msgSize;

    private final long storeTimestamp;
    private final long consumeQueueOffset;

    public QueueRequest(String topic, int queueId, long commitLogOffset, int msgSize, long storeTimestamp, long consumeQueueOffset) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.success = true;
    }

    private final boolean success;

    public QueueRequest(int size, boolean success) {
        // 1
        this.topic = "";
        // 2
        this.queueId = 0;
        // 3
        this.commitLogOffset = 0;
        // 4
        this.msgSize = size;
        // 5

        // 6
        this.storeTimestamp = 0;
        // 7
        this.consumeQueueOffset = 0;
        // 8

        this.success = success;
    }


    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }

    public boolean isSuccess() {
        return success;
    }
}
