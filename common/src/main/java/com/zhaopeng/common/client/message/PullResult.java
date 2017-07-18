package com.zhaopeng.common.client.message;

import com.zhaopeng.common.client.enums.PullStatus;
import com.zhaopeng.remoting.protocol.JsonSerializable;

import java.util.List;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public class PullResult extends JsonSerializable {
    private  PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    //private List<MessageInfo> msgFoundList;

    private List<Message> messages;

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

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

 /*   public List<MessageInfo> getMsgFoundList() {
        return msgFoundList;
    }

    public void setMsgFoundList(List<MessageInfo> msgFoundList) {
        this.msgFoundList = msgFoundList;
    }
*/
    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }
}
