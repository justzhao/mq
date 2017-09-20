package com.zhaopeng.mq.producer;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.impl.DefaultMQProducer;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;

import java.io.UnsupportedEncodingException;

/**
 * Created by zhaopeng on 2017/9/20.
 */
public class ProducerStart {




    public static void main(String args[]) throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {

        NettyClientConfig clientConfig = new NettyClientConfig();


        String addr = "127.0.0.1:9876";

        DefaultMQProducer producer = new DefaultMQProducer(clientConfig, addr);
        producer.init();

        Message message = new Message();
        message.setTopic("zhaopeng");
        for (int i = 0; i < 100; i++) {
            message.setBody(("hello world " + i).getBytes("utf-8"));
            producer.send(message);
        }

    }
}
