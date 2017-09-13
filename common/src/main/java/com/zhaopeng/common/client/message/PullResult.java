package com.zhaopeng.common.client.message;

import com.zhaopeng.common.client.enums.PullStatus;
import com.zhaopeng.common.message.PullMessage;
import com.zhaopeng.remoting.protocol.JsonSerializable;

import java.util.List;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public class PullResult extends JsonSerializable {
    private PullStatus pullStatus;
    private  long nextBeginOffset;
    private  long minOffset;
    private  long maxOffset;
    private List<PullMessage> msgs;


    public PullResult() {
        this.nextBeginOffset = 0;
        this.minOffset = 0;
        this.maxOffset = 0;
    }

    public PullResult(PullStatus pullStatus) {
        this.pullStatus = pullStatus;
        this.nextBeginOffset = 0;
        this.minOffset = 0;
        this.maxOffset = 0;
    }

    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset) {
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
    }

    public PullStatus getPullStatus() {
        return pullStatus;
    }

    public void setPullStatus(PullStatus pullStatus) {
        this.pullStatus = pullStatus;
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

    public List<PullMessage> getMsgs() {
        return msgs;
    }

    public void setMsgs(List<PullMessage> msgs) {
        this.msgs = msgs;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }
}
