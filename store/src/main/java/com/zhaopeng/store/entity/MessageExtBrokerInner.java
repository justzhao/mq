package com.zhaopeng.store.entity;

import com.zhaopeng.common.client.message.Message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by zhaopeng on 2017/7/31.
 */
public class MessageExtBrokerInner extends Message {

    private long bodyLength;

    private String host;

    private int queueId;

    private int storeSize;

    private long queueOffset;

    private long bornTimestamp;


    private long storeTimestamp;

    private String msgId;
    private long commitLogOffset;
    private int bodyCRC;
    private int reconsumeTimes;



    public long getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(long bodyLength) {
        this.bodyLength = bodyLength;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }


    public int getQueueId() {
        return queueId;
    }


    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
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

    public int getBodyCRC() {
        return bodyCRC;
    }

    public void setBodyCRC(int bodyCRC) {
        this.bodyCRC = bodyCRC;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public ByteBuffer getBornHostBytes() {
        return socketAddress2ByteBuffer(this.host);
    }
    public static ByteBuffer socketAddress2ByteBuffer(String host) {
        String []hosts=host.split(":");
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        if(host!=null&&host.length()==2) {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(hosts[0],Integer.parseInt(hosts[1]));

            byteBuffer.put(inetSocketAddress.getAddress().getAddress());
            byteBuffer.putInt(inetSocketAddress.getPort());
            byteBuffer.flip();
        }
        return byteBuffer;
    }

    @Override
    public String toString() {
        return "MessageExtBrokerInner{" +
                "bodyLength=" + bodyLength +
                ", host='" + host + '\'' +
                ", queueId=" + queueId +
                ", storeSize=" + storeSize +
                ", queueOffset=" + queueOffset +
                ", bornTimestamp=" + bornTimestamp +
                ", storeTimestamp=" + storeTimestamp +
                ", msgId='" + msgId + '\'' +
                ", commitLogOffset=" + commitLogOffset +
                ", bodyCRC=" + bodyCRC +
                ", reconsumeTimes=" + reconsumeTimes +
                '}';
    }
}
