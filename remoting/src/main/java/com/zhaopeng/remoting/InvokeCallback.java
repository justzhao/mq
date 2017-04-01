package com.zhaopeng.remoting;

import com.zhaopeng.remoting.netty.ResponseFuture;

/**
 * Created by zhaopeng on 2017/4/1.
 *  netty通信完成之后的回调
 */
public interface InvokeCallback {
    public void operationComplete(final ResponseFuture responseFuture);
}
