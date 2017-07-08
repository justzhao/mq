package com.zhaopeng.mq;

/**
 * Created by zhaopeng on 2017/4/22.
 */
public class BrokerStart {
    public static void main(String args[]) {

        BrokerController controller = new BrokerController("127.0.0.1");
        controller.start();


    }
}
