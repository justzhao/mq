package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.PullResult;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public interface PullCallback {
    public void onSuccess(final PullResult pullResult);

    public void onException(final Throwable e);
}
