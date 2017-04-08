package com.zhaopeng.namesrv;

import com.zhaopeng.common.namesrv.NameSrvConfig;
import com.zhaopeng.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhaopeng on 2017/4/5.
 */
public class NameSrvController {

    private static final Logger log = LoggerFactory.getLogger(NameSrvController.class);

    private final NameSrvConfig nameSrvConfig;

    private final NettyServerConfig nettyServerConfig;

    public NameSrvController(NameSrvConfig nameSrvConfig, NettyServerConfig nettyServerConfig) {
        this.nameSrvConfig = nameSrvConfig;
        this.nettyServerConfig = nettyServerConfig;
    }


    public NameSrvConfig getNameSrvConfig() {
        return nameSrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public boolean init(){


        return true;
    }

    public boolean start(){


        return  true;
    }

    public void  shutdown(){

    }


}
