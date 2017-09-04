package com.zhaopeng.mq.start;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.impl.DefaultMQProducer;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;

import java.io.UnsupportedEncodingException;

/**
 * Created by zhaopeng on 2017/7/10.
 */
public class ProdocerQuickStart {

    public static void main(String args[]) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {


        NettyClientConfig clientConfig = new NettyClientConfig();


        String addr = "127.0.0.1:9876";

        DefaultMQProducer producer = new DefaultMQProducer(clientConfig, addr);
        producer.init();

        Message message = new Message();
        message.setTopic("zhaopeng");
        for (int i = 0; i < 1; i++) {
            message.setBody(("hello world " + i).getBytes("utf-8"));
            producer.send(message);
        }


    }
}
