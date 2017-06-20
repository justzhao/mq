package com.zhaopeng.mq;

import com.zhaopeng.common.protocol.body.RegisterBrokerResult;
import com.zhaopeng.remoting.netty.NettyClient;

/**
 * Created by zhaopeng on 2017/6/20.
 */
public class BrokerOutApi {

    private final  NettyClient nettyClient;

    public BrokerOutApi(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }




    public RegisterBrokerResult registerBrokerAll(//
                                                  final String clusterName, // 1
                                                  final String brokerAddr, // 2
                                                  final String brokerName, // 3
                                                  final long brokerId, // 4
                                                  final boolean oneway,// 5
                                                  final int timeoutMills// 6
    ) {

        RegisterBrokerResult registerBrokerResult=new RegisterBrokerResult();

        return registerBrokerResult;

    }
}
