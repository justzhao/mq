package com.zhaopeng.namesrv.route;

import com.zhaopeng.common.protocol.body.RegisterBrokerInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerResult;
import io.netty.channel.Channel;

/**
 * Created by zhaopeng on 2017/4/9.
 */
public class RouteInfoManager {



    /**
     * 关闭channel
     * @param remoteAddr
     * @param channel
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {

    }

    public  void scanAliveBroker(){

    }

    public void  printAllConfPeriodically(){

    }

    public RegisterBrokerResult registerBroker(RegisterBrokerInfo brokerInfo){

        RegisterBrokerResult result =new RegisterBrokerResult();

        result.setServerAddr(brokerInfo.getServerAddr());
        return result;
    }



}
