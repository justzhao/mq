package com.zhaopeng.mq.producer;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;

/**
 * Created by zhaopeng on 2017/5/6.
 */
public interface MQProducer {

    public SendResult send(Message msg,long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
}
