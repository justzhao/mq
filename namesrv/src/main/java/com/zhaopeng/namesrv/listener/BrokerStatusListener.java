package com.zhaopeng.namesrv.listener;

import com.zhaopeng.namesrv.NameSrvController;
import com.zhaopeng.remoting.ChannelEventListener;
import io.netty.channel.Channel;

/**
 * Created by zhaopeng on 2017/4/9.
 */
public class BrokerStatusListener implements ChannelEventListener {

    private NameSrvController nameSrvController;

    public BrokerStatusListener(NameSrvController nameSrvController) {
        this.nameSrvController = nameSrvController;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {

    }
}
