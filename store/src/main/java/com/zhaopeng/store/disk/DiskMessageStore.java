package com.zhaopeng.store.disk;

import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.remoting.common.SystemClock;
import com.zhaopeng.store.ConsumeQueue;
import com.zhaopeng.store.MessageStore;
import com.zhaopeng.store.commit.CommitLog;
import com.zhaopeng.store.config.MessageStoreConfig;
import com.zhaopeng.store.config.StorePathConfigHelper;
import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.entity.PutMessageResult;
import com.zhaopeng.store.entity.QueueRequest;
import com.zhaopeng.store.entity.enums.PutMessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaopeng on 2017/7/27.
 */
public class DiskMessageStore implements MessageStore {

    private static final Logger logger = LoggerFactory.getLogger(ServiceThread.class);


    private final MessageStoreConfig messageStoreConfig;
    private final CommitLog commitLog;

    private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;


    private final FlushConsumeQueueService flushConsumeQueueService;

    private final CleanCommitLogService cleanCommitLogService;

    private final CleanConsumeQueueService cleanConsumeQueueService;

    private final ReputMessageService reputMessageService;

    private final IndexService indexService;

    private final SystemClock systemClock;

    private volatile boolean shutdown = true;

    private List<TopicInfo> topics;

    public void DiskMessageStore() {
    }

    public void load() {

        this.commitLog.load();
        this.loadConsumeQueue();
        this.recover();
        this.shutdown = false;

    }

    public void recover() {
        this.recoverConsumeQueue();
        this.commitLog.recoverNormally();
        this.recoverTopicQueueTable();
    }

    public void recoverConsumeQueue() {
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
            }
        }
    }

    public DiskMessageStore() {
        this.messageStoreConfig = new MessageStoreConfig();

        consumeQueueTable = new ConcurrentHashMap<>();
        indexService = new IndexService();
        cleanConsumeQueueService = new CleanConsumeQueueService();
        cleanCommitLogService = new CleanCommitLogService();
        flushConsumeQueueService = new FlushConsumeQueueService();
        reputMessageService = new ReputMessageService();
        systemClock = new SystemClock(1);
        commitLog = new CommitLog(messageStoreConfig, this);
        topics = new ArrayList<>();

    }


    @Override
    public List<TopicInfo> getTopicInfosFromConsumeQueue() {
        return topics;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public FlushConsumeQueueService getFlushConsumeQueueService() {
        return flushConsumeQueueService;
    }

    public CleanCommitLogService getCleanCommitLogService() {
        return cleanCommitLogService;
    }

    public CleanConsumeQueueService getCleanConsumeQueueService() {
        return cleanConsumeQueueService;
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public void setShutdown(boolean shutdown) {
        this.shutdown = shutdown;
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    /**
     * 这里设计有问题，添加进去就是二进制，取出来也是二进制，
     * 附加信息放在head中，比如下一次拉取的offset
     *
     * @param pull
     * @return
     */
    @Override
    public Message getMessage(PullMesageInfo pull) {
        GetMessageResult getMessageResult = this.getMessage(pull.getTopic(), pull.getQueueId(), pull.getQueueOffset(), pull.getMaxMsgNums());
        Message message = new Message();
        //  message.setBody(r);

        message.setTopic(pull.getTopic());

        message.setCommitLogOffset(pull.getCommitOffset());

        return message;
    }

    @Override
    public GetMessageResult getMessageContent(PullMesageInfo pull) {


        return getMessage(pull.getTopic(), pull.getQueueId(), pull.getCommitOffset(), pull.getMaxMsgNums());

    }


    @Override
    public PutMessageResult addMessage(SendMessage sendMessage) {
        if (this.shutdown) {
            logger.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE);
        }
        if (sendMessage.getTopic().length() > Byte.MAX_VALUE) {
            logger.warn("putMessage message topic length too long " + sendMessage.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL);
        }
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY);
        }
        MessageExtBrokerInner msg = new MessageExtBrokerInner();

        msg.setBody(sendMessage.getMsg().getBody());
        msg.setTopic(sendMessage.getTopic());
        msg.setQueueId(sendMessage.getQueueId());
        msg.setProperties(sendMessage.getMsg().getProperties());
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setHost(sendMessage.getHost());

        PutMessageResult result = this.commitLog.putMessage(msg);

        return result;
    }


    public GetMessageResult getMessage(String topic, int queueId, long offset, int maxMsgNums) {
        if (this.shutdown) {
            logger.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();
        // 每次请求都会去先获取consumerQueue，consumerQueue 保存着消息在messageLog的物理位置信息
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = consumeQueue.getMinOffsetInQuque();
            maxOffset = consumeQueue.getMaxOffsetInQuque();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {
                // 根据consumerQueue的offset获取 所有消息的位置信息

                SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        int i = 0;
                        final int MaxFilterMessageCount = 16000;

                        for (; i < bufferConsumeQueue.getSize() && i < MaxFilterMessageCount; i += ConsumeQueue.CQStoreUnitSize) {
                            // messgeQueue  使用12个字节存取消息的物理位置信息和长度信息
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();

                            if (isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(), true)) {
                                break;
                            }

                            SelectMapedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (selectResult != null) {
                                getResult.addMessage(selectResult);
                                status = GetMessageStatus.FOUND;
                                nextPhyFileStartOffset = Long.MIN_VALUE;
                                if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                    if (offsetPy < nextPhyFileStartOffset)
                                        continue;
                                }

                            }
                            nextBeginOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);

                        }

                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    logger.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }
        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;

    }


    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        nextOffset = newOffset;

        return nextOffset;
    }


    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        if (diff < 10000000 //
                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills()) {
            return true;
        }

        return false;
    }

    @Override
    public void start() {


        flushConsumeQueueService.start();

        cleanCommitLogService.start();

        reputMessageService.start();

        commitLog.start();

        shutdown = false;
    }

    @Override
    public void shutDown() {

        shutdown = true;
    }


    private boolean loadConsumeQueue() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        // 这里需要注册topic信息到namesrv 带上 brokerConfig 的信息

        //
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    int queueNum = 0;
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());

                        } catch (NumberFormatException e) {
                            continue;
                        }
                        queueNum++;
                        ConsumeQueue logic = new ConsumeQueue(//
                                topic, //
                                queueId, //
                                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), //
                                this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(), //
                                this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }

                    TopicInfo topicInfo = new TopicInfo();
                    topicInfo.setTopicName(topic);
                    topicInfo.setWriteQueueNums(queueNum);
                    topicInfo.setReadQueueNums(queueNum);
                    this.topics.add(topicInfo);
                }


            }
        }

        logger.info("load logics queue all over, OK");

        return true;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentHashMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }


    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQuque());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }

    public List<TopicInfo> getTopics() {
        return topics;
    }

    public void setTopics(List<TopicInfo> topics) {
        this.topics = topics;
    }

    class FlushConsumeQueueService extends ServiceThread {

        private static final int RetryTimesOver = 3;

        @Override
        public String getServiceName() {
            return "FlushConsumeQueueService";
        }

        private void doFlush(int retryTimes) {
            int flushConsumeQueueLeastPages = DiskMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();
            if (retryTimes == RetryTimesOver) {
                flushConsumeQueueLeastPages = 0;
            }

            ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables = DiskMessageStore.this.consumeQueueTable;
            for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.commit(flushConsumeQueueLeastPages);
                    }
                }
            }


        }


        @Override
        public void run() {

        }
    }

    class CleanCommitLogService extends ServiceThread {

        @Override
        public String getServiceName() {
            return "CleanCommitLogService";
        }

        @Override
        public void run() {

        }
    }

    class CleanConsumeQueueService extends ServiceThread {

        @Override
        public String getServiceName() {
            return "CleanConsumeQueueService";
        }

        @Override
        public void run() {

        }
    }

    class IndexService extends ServiceThread {

        @Override
        public String getServiceName() {
            return "IndexService";
        }

        @Override
        public void run() {

        }
    }

    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentHashMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentHashMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<>(128);
            ConcurrentHashMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(//
                    topic, //
                    queueId, //
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), //
                    messageStoreConfig.getMapedFileSizeConsumeQueue(), //
                    this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }


    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if ((messageTotal + 1) >= maxMsgNums) {
            return true;
        }


        return false;
    }


    public void putMessagePostionInfo(String topic, int queueId, long offset, int size, long storeTimestamp,
                                      long logicOffset) {
        ConsumeQueue cq = this.findConsumeQueue(topic, queueId);
        cq.putMessagePostionInfoWrapper(offset, size, storeTimestamp, logicOffset);
    }

    public void doAddConsumeQueueRequest(QueueRequest req) {
        reputMessageService.putRequest(req);
    }

    public void doDispatch(QueueRequest req) {

        //

        DiskMessageStore.this.putMessagePostionInfo(req.getTopic(), req.getQueueId(), req.getCommitLogOffset(), req.getMsgSize(),
                req.getStoreTimestamp(), req.getConsumeQueueOffset());

    }


    class ReputMessageService extends ServiceThread {


        private volatile Queue<QueueRequest> request = new LinkedList<>();

        public void putRequest(final QueueRequest request) {
            synchronized (this) {
                this.request.offer(request);
                if (!this.hasNotified) {
                    this.hasNotified = true;
                    this.notify();
                }
            }
        }


        private void docommitRequest() {
            if (this.hasNotified && !this.request.isEmpty()) {

                QueueRequest r = request.poll();
                DiskMessageStore.this.doDispatch(r);

            }

        }


        @Override
        public void shutdown() {
            for (int i = 0; i < 50; i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }


            super.shutdown();
        }


        @Override
        public void run() {
            DiskMessageStore.logger.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    Thread.sleep(1);
                    this.docommitRequest();
                } catch (Exception e) {
                    DiskMessageStore.logger.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DiskMessageStore.logger.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }


    }
}
