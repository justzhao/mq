package com.zhaopeng.mq.api.service;

import com.zhaopeng.remoting.protocol.RemotingCommand;

/**
 * Created by zhaopeng on 2017/6/16.
 */
public interface SendService {

    public void sendMessage(RemotingCommand remotingCommand, long timeout);
}
