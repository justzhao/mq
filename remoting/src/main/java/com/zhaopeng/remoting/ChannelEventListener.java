package com.zhaopeng.remoting;

import io.netty.channel.Channel;

/**
 * Created by zhaopeng on 2017/4/3.
 *  用来监听 netty连接中Channel 状态变化的监听器
 */
public interface ChannelEventListener {

    void onChannelConnect(final String remoteAddr, final Channel channel);


    void onChannelClose(final String remoteAddr, final Channel channel);


    void onChannelException(final String remoteAddr, final Channel channel);

    /**
     * 当有Channel闲置的时候触发
     * @param remoteAddr
     * @param channel
     */
    void onChannelIdle(final String remoteAddr, final Channel channel);
}
