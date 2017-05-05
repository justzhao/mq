package com.zhaopeng.mq;

import com.zhaopeng.mq.exception.MQClientException;

/**
 * Created by zhaopeng on 2017/4/23.
 */
public interface MQOperation {


    /**
     * 创建一个topic
     * @param key
     * @param newTopic
     * @param queueNum
     * @throws MQClientException
     */
    void createTopic(final String key, final String newTopic, final int queueNum)
            throws MQClientException;

}
