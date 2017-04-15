package com.zhaopeng.common.protocol.route;

/**
 * Created by zhaopeng on 2017/4/13.
 */
public class QueueData implements Comparable<QueueData> {

    private String brokerName;
    private int readQueueNums;
    private int writeQueueNums;
    private int perm;


    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
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

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueueData)) return false;

        QueueData queueData = (QueueData) o;

        if (readQueueNums != queueData.readQueueNums) return false;
        if (writeQueueNums != queueData.writeQueueNums) return false;
        return brokerName.equals(queueData.brokerName);

    }

    @Override
    public int hashCode() {
        int result = brokerName.hashCode();
        result = 31 * result + readQueueNums;
        result = 31 * result + writeQueueNums;
        return result;
    }
}
