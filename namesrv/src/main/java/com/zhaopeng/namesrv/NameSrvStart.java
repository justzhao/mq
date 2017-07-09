package com.zhaopeng.namesrv;

import com.zhaopeng.common.All;
import com.zhaopeng.common.namesrv.NameSrvConfig;
import com.zhaopeng.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhaopeng on 2017/4/5.
 * <p>
 * 启动消息队列的服务发现和注册
 */
public class NameSrvStart {

    final Logger log = LoggerFactory.getLogger(NameSrvStart.class);


    public static void main(String args[]) {
        NameSrvStart srvStart = new NameSrvStart();
        srvStart.startUp();
    }


    public void startUp() {
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setPort(9876);
        NameSrvConfig nameSrvConfig = new NameSrvConfig();
        try {
            if (null == nameSrvConfig.getMqHome()) {
                System.out.println("Please set the " + All.MQ_HOME_ENV
                        + " variable in your environment to match the location of the MQ installation");
                System.exit(0);
            }
            NameSrvController nameSrvController = new NameSrvController(nameSrvConfig, nettyServerConfig);
            if (!nameSrvController.init()) {
                nameSrvController.shutdown();
                System.exit(0);
            }
            nameSrvController.start();
            log.info("The Name Server boot success.");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }

}
