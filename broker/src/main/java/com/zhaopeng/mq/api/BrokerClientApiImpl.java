package com.zhaopeng.mq.api;

import com.zhaopeng.common.protocol.body.RegisterBrokerInfo;
import com.zhaopeng.remoting.netty.NettyClient;

/**
 * Created by zhaopeng on 2017/5/22.
 */
public class BrokerClientApiImpl {

    private final NettyClient nettyClient;

    // nameSrv 的地址
    private final String addr;

    public BrokerClientApiImpl(NettyClient nettyClient, String addr) {
        this.nettyClient = nettyClient;
        this.addr = addr;
    }

    public void sendRegistorBrokerMessage(long timeout,String brokerName,String brokerAddr,Long brokerId){
        RegisterBrokerInfo registerBrokerInfo=new RegisterBrokerInfo();
        registerBrokerInfo.setBrokerId(brokerId);
        registerBrokerInfo.setBrokerName(brokerName);
        registerBrokerInfo.setServerAddr(brokerAddr);

    }
}
