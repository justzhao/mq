package com.zhaopeng.store.commit;

import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.entity.PutMessageResult;

/**
 * Created by zhaopeng on 2017/7/27.
 */
public class CommitLog {
    private volatile long confirmOffset = -1L;
    private volatile long beginTimeInLock = 0;

    public long getConfirmOffset() {
        return confirmOffset;
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    public void setBeginTimeInLock(long beginTimeInLock) {
        this.beginTimeInLock = beginTimeInLock;
    }

    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {


        return null;
    }


}
