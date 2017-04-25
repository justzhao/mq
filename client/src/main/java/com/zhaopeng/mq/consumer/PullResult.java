package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.enums.PullStatus;
import com.zhaopeng.common.client.message.MessageInfo;

import java.util.List;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public class PullResult {
    private final PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    private List<MessageInfo> msgFoundList;

    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset) {
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
    }

    public PullStatus getPullStatus() {
        return pullStatus;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public List<MessageInfo> getMsgFoundList() {
        return msgFoundList;
    }

    public void setMsgFoundList(List<MessageInfo> msgFoundList) {
        this.msgFoundList = msgFoundList;
    }
}
