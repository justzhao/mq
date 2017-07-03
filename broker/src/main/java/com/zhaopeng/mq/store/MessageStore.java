package com.zhaopeng.mq.store;

import com.zhaopeng.common.client.message.Message;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaopeng on 2017/6/28.
 */
public class MessageStore {


    // 计算总数
    AtomicInteger count = new AtomicInteger(0);

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
}
