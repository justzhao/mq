package com.zhaopeng.mq.consumer.push;

import com.zhaopeng.common.client.message.MessageModel;
import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.message.ProcessQueue;
import com.zhaopeng.common.client.message.PullRequest;
import com.zhaopeng.mq.consumer.RebalanceImpl;
import com.zhaopeng.mq.consumer.impl.MQPushClientOperation;
import com.zhaopeng.mq.store.AllocateMessageQueueStrategy;

import java.util.List;
import java.util.Set;

/**
 * Created by zhaopeng on 2017/10/15.
 */
public class RebalancePushImpl extends RebalanceImpl {
    public RebalancePushImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQPushClientOperation mQClientFactory) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {

    }

    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        return false;
    }



    @Override
    public void removeDirtyOffset(MessageQueue mq) {

    }

    @Override
    public long computePullFromWhere(MessageQueue mq) {
        return 0;
    }

    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {

    }


}
