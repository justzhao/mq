package com.zhaopeng.store.commit;

import com.zhaopeng.store.entity.enums.AppendMessageStatus;

/**
 * Created by zhaopeng on 2017/8/7.
 */
public class AppendMessageResult {
    // Return code
    private AppendMessageStatus status;
    // Where to start writings?'????????????????????
    private long wroteOffset;
    // Write Bytes
    private int wroteBytes;
    // Message ID
    private String msgId;
    // Message storage timestamp
    private long storeTimestamp;
    // Consume queue's offset(step by one)
    private long logicsOffset;
    private long pagecacheRT = 0;

    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getLogicsOffset() {
        return logicsOffset;
    }

    public void setLogicsOffset(long logicsOffset) {
        this.logicsOffset = logicsOffset;
    }

    public long getPagecacheRT() {
        return pagecacheRT;
    }

    public void setPagecacheRT(long pagecacheRT) {
        this.pagecacheRT = pagecacheRT;
    }
}
