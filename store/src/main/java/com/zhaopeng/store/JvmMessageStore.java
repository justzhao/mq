package com.zhaopeng.store;

import com.google.common.collect.Maps;
import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.body.PullMesageInfo;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaopeng on 2017/6/28.
 */
public class JvmMessageStore implements MessageStore {


    // 计算总数
    AtomicInteger count = new AtomicInteger(0);


    Map<Integer, Queue<Message>> queueMap = Maps.newConcurrentMap();


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

    @Override
    public Message getMessage(PullMesageInfo pull) {
        int queueId = pull.getQueueId();
        Queue<Message> queue = queueMap.get(queueId);
        return queue.poll();
    }

    @Override
    public void addMessage(SendMessage sendMessage) {

        int queueId = sendMessage.getQueueId();

        Queue<Message> queue = queueMap.get(queueId);
        if (queue == null) {
            queue = new LinkedList<>();
            queueMap.put(queueId, queue);
        }
        queue.offer(sendMessage.getMsg());

    }
}
