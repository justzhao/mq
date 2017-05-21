package com.zhaopeng.mq;

import com.zhaopeng.mq.config.BrokerConfig;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.netty.NettyClientConfig;
import com.zhaopeng.remoting.netty.NettyServer;
import com.zhaopeng.remoting.netty.NettyServerConfig;

/**
 * Created by zhaopeng on 2017/4/22.
 */
public class BrokerController {

    private NettyServer nettyServer;

    private NettyClient nettyClient;


    private BrokerConfig brokerConfig;

    private NettyClientConfig nettyClientConfig;

    private NettyServerConfig nettyServerConfig;

    public BrokerController(){
        nettyClient=new NettyClient(nettyClientConfig);
        nettyServer=new NettyServer(nettyServerConfig,null);

    }

    public void start() {

      //  nettyServer.registerProcessor();
        nettyClient.start();

        nettyServer.start();



    }


}
