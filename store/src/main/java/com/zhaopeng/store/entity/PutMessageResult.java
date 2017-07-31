package com.zhaopeng.store.entity;

import com.zhaopeng.store.entity.enums.PutMessageStatus;

/**
 * Created by zhaopeng on 2017/7/31.
 */
public class PutMessageResult {

    private PutMessageStatus putMessageStatus;

    public PutMessageResult() {
    }

    public PutMessageResult(PutMessageStatus putMessageStatus) {

        this.putMessageStatus = putMessageStatus;
    }
}
