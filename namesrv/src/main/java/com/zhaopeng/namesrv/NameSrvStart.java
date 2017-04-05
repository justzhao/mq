package com.zhaopeng.namesrv;

import com.zhaopeng.remoting.netty.NettyServerConfig;

/**
 * Created by zhaopeng on 2017/4/5.
 * <p>
 * 启动消息队列的服务发现和注册
 */
public class NameSrvStart {

    public static void main(String args[]) {
        startUp();
    }

    private static void startUp() {
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setPort(9876);

    }

}
