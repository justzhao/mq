package com.zhaopeng.mq.consumer.impl;

import com.zhaopeng.common.UtilAll;
import com.zhaopeng.common.client.message.ConsumeFromWhere;
import com.zhaopeng.common.client.message.MessageModel;
import com.zhaopeng.mq.consumer.AbstractMQConsumer;
import com.zhaopeng.mq.consumer.MQPushConsumer;
import com.zhaopeng.mq.consumer.MQPushConsumerInner;
import com.zhaopeng.mq.consumer.listener.MessageListener;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.rebalance.AllocateMessageQueueAveragely;
import com.zhaopeng.mq.store.AllocateMessageQueueStrategy;
import com.zhaopeng.mq.store.OffsetStore;
import com.zhaopeng.remoting.netty.NettyClientConfig;

/**
 * Created by zhaopeng on 2017/10/11.
 */
public class DefaultMQPushConsumer extends AbstractMQConsumer implements MQPushConsumer {


    protected final transient MQPushConsumerInner mqPushConsumerInner;


    /**
     * 集群消费和广播消费，集群消费：同一个分组的消费者会共享他们的订阅的主题，也就是同一个topic
     * 的消息只会被同一个分组的一个客户端消费。
     * 广播消费：每个客户端都会消费自己订阅的信息
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;


    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;


    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));


    /**
     * 用来决定怎么分配mq给每个消费者
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;


    /**
     * 消费消息的监听类，开发者实现
     */
    private MessageListener messageListener;

    /**
     * 用来存放消费进度的类
     */
    private OffsetStore offsetStore;

    /**
     * Minimum consumer thread number
     */
    private int consumeThreadMin = 20;

    /**
     * Max consumer thread number
     */
    private int consumeThreadMax = 64;

    /**
     * Threshold for dynamic adjustment of the number of thread pool
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * Concurrently max span offset.it has no effect on sequential consumption
     */
    private int consumeConcurrentlyMaxSpan = 2000;

    /**
     * Flow control threshold
     */
    private int pullThresholdForQueue = 1000;

    /**
     * Message pull Interval
     */
    private long pullInterval = 0;

    /**
     * 批量消费个数
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * 批量拉取消息消息的个数
     */
    private int pullBatchSize = 32;

    /**
     * Whether update subscription relationship when every pull
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    /**
     * Max re-consume times. -1 means 16 times.
     * </p>
     * <p>
     * If messages are re-consumed more than {@link #maxReconsumeTimes} before success, it's be directed to a deletion
     * queue waiting.
     */
    private int maxReconsumeTimes = -1;

    /**
     * Suspending pulling time for cases requiring slow pulling like flow-control scenario.
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    private long consumeTimeout = 15;


    // namesrv的地址
    private String addr;


    public DefaultMQPushConsumer(NettyClientConfig config,String addr) {
        super(config);

        this.allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
        this.addr = addr;
        this.mqPushConsumerInner = new MQPushConsumerInnerImpl(this, addr);
    }


    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.mqPushConsumerInner.registerMessageListener(messageListener);

    }

    @Override
    public void subscribe(String topic) throws MQClientException {
        mqPushConsumerInner.subscribe(topic);

    }


    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }

    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }


    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }


    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }


}
