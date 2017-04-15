package com.zhaopeng.common;

/**
 * Created by zhaopeng on 2017/4/12.
 */
public class TopicInfo {
    public static int defaultReadQueueNums = 16;
    public static int defaultWriteQueueNums = 16;
    private String topicName;
    private int readQueueNums = defaultReadQueueNums;
    private int writeQueueNums = defaultWriteQueueNums;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }
}
