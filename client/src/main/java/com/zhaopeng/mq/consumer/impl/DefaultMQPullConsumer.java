package com.zhaopeng.mq.consumer.impl;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.mq.consumer.AbstractMQConsumer;
import com.zhaopeng.mq.consumer.MQPullConsumer;
import com.zhaopeng.mq.consumer.PullResult;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public class DefaultMQPullConsumer extends AbstractMQConsumer implements MQPullConsumer {


    private final MQPullClientOperation mqPullClientOperation;

    // namesrv的地址
    private String namesrv;

    public DefaultMQPullConsumer(NettyClientConfig nettyClientConfig, String addr) {
        super(nettyClientConfig);
        mqPullClientOperation = new MQPullClientOperation(nettyClient,addr);
    }

    public void init(){
        initLog();
    }

    public void initLog(){
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        try {
            configurator.doConfigure("classpath:logback.xml");
        } catch (JoranException e) {
            e.printStackTrace();
        }
        final Logger log = LoggerFactory.getLogger(DefaultMQPullConsumer.class);
        log.info("hehedada");
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return mqPullClientOperation.pull(mq, subExpression, offset, maxNums);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        mqPullClientOperation.createTopic(key, newTopic, queueNum);
    }


    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return mqPullClientOperation.fetchSubscribeMessageQueues(topic);
    }
}
