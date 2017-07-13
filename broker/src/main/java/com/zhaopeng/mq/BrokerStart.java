package com.zhaopeng.mq;

/**
 * Created by zhaopeng on 2017/4/22.
 */
public class BrokerStart {
    public static void main(String args[]) {

        String namesrv = "127.0.0.1:9876";
        BrokerController controller = new BrokerController(namesrv);
        controller.start();


    }
}
