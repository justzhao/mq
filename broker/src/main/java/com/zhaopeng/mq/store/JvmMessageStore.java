package com.zhaopeng.mq.store;

import com.google.common.collect.Maps;
import com.zhaopeng.common.client.message.Message;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaopeng on 2017/6/28.
 */
public class JvmMessageStore {


    // 计算总数
    AtomicInteger count = new AtomicInteger(0);


    Map<Integer,Queue<Message>>  queueMap= Maps.newConcurrentMap();

    Queue<Message> queue = new LinkedList<>();


    public Queue<Message> getQueue() {
        return queue;
    }

    public void setQueue(Queue<Message> queue) {
        this.queue = queue;
    }

    public AtomicInteger getCount() {
        return count;
    }

    public void setCount(AtomicInteger count) {
        this.count = count;
    }

    public int increatCount() {
        return count.incrementAndGet();
    }

    public Map<Integer, Queue<Message>> getQueueMap() {
        return queueMap;
    }

    public void setQueueMap(Map<Integer, Queue<Message>> queueMap) {
        this.queueMap = queueMap;
    }
}
