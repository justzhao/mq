package com.zhaopeng.store.disk;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.common.store.MessageOffsetConstant;
import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.remoting.common.SystemClock;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.store.ConsumeQueue;
import com.zhaopeng.store.MessageStore;
import com.zhaopeng.store.commit.CommitLog;
import com.zhaopeng.store.config.MessageStoreConfig;
import com.zhaopeng.store.config.StorePathConfigHelper;
import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.entity.PutMessageResult;
import com.zhaopeng.store.entity.enums.PutMessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaopeng on 2017/7/27.
 */
public class DiskMessageStore implements MessageStore {

    private static final Logger logger = LoggerFactory.getLogger(ServiceThread.class);


    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    private final CommitLog commitLog;

    private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;


    private final FlushConsumeQueueService flushConsumeQueueService;

    private final CleanCommitLogService cleanCommitLogService;

    private final CleanConsumeQueueService cleanConsumeQueueService;

    private final IndexService indexService;

    private final SystemClock systemClock;

    private volatile boolean shutdown = true;

    public void DiskMessageStore() {

    }

    public DiskMessageStore(final MessageStoreConfig messageStoreConfig) throws IOException {
        this.messageStoreConfig = messageStoreConfig;
        commitLog = new CommitLog();
        consumeQueueTable = new ConcurrentHashMap<>();
        indexService = new IndexService();
        cleanConsumeQueueService = new CleanConsumeQueueService();
        cleanCommitLogService = new CleanCommitLogService();
        flushConsumeQueueService = new FlushConsumeQueueService();
        systemClock = new SystemClock(1);


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

        final byte[] r = this.readGetMessageResult(getMessageResult);

        Message message = new Message();
        message.setBody(r);

        message.setTopic(pull.getTopic());

        message.setCommitLogOffset(pull.getCommitOffset());

        return message;
    }

    @Override
    public RemotingCommand getMessageContent(PullMesageInfo pull) {


        RemotingCommand response = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);

        GetMessageResult result = getMessage(pull.getTopic(), pull.getQueueId(), pull.getCommitOffset(), pull.getMaxMsgNums());
        byte[] bytes = readGetMessageResult(result);
        response.getExtFields().put(MessageOffsetConstant.MINOFFSET, result.getMinOffset());
        response.getExtFields().put(MessageOffsetConstant.MAXOFFSET, result.getMaxOffset());
        response.getExtFields().put(MessageOffsetConstant.NEXTBEGINOFFSET, result.getNextBeginOffset());


        response.setBody(bytes);
        return response;
    }


    private byte[] readGetMessageResult(final GetMessageResult getMessageResult) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                byteBuffer.put(bb);
            }
        } finally {
            getMessageResult.release();
        }
        return byteBuffer.array();
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
        long beginTime = this.getSystemClock().now();
        MessageExtBrokerInner msg = new MessageExtBrokerInner();

        ConcurrentHashMap<Integer/* queueId */, ConsumeQueue> queueMap = consumeQueueTable.get(sendMessage.getTopic());
        if (queueMap == null) {
            queueMap = new ConcurrentHashMap<>();
            consumeQueueTable.put(sendMessage.getTopic(), queueMap);
        }
        ConsumeQueue consumeQueue = queueMap.get(sendMessage.getQueueId());
        if (consumeQueue == null) {
            consumeQueue = new ConsumeQueue(//
                    sendMessage.getTopic(), //
                    sendMessage.getQueueId(), //
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), //
                    this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(), this);
            queueMap.put(sendMessage.getQueueId(), consumeQueue);
        }

        PutMessageResult result = this.commitLog.putMessage(msg);
        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 1000) {
            logger.info("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBodyLength());
        }
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

                            // messgeQueue  使用20个字节存取消息的物理位置信息和长度信息
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();

                            SelectMapedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (selectResult != null) {
                                getResult.addMessage(selectResult);
                                status = GetMessageStatus.FOUND;
                                nextPhyFileStartOffset = Long.MIN_VALUE;

                                if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                    if (offsetPy < nextPhyFileStartOffset)
                                        continue;
                                }

                                if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(), true)) {
                                    break;
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

        shutdown = false;
    }

    @Override
    public void shutDown() {

        shutdown = true;
    }


    class FlushConsumeQueueService extends ServiceThread {
        @Override
        public String getServiceName() {
            return "FlushConsumeQueueService";
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


    public void putMessagePostionInfo(String topic, int queueId, long offset, int size, long tagsCode, long storeTimestamp,
                                      long logicOffset) {
        ConsumeQueue cq = this.findConsumeQueue(topic, queueId);
        cq.putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset);
    }



    class ReputMessageService extends ServiceThread {

        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }

            if (this.isCommitLogAvailable()) {
                logger.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        DiskMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        public long behind() {
            return DiskMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }


        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DiskMessageStore.this.commitLog.getMaxOffset();
        }


        private void doReput() {

        }


        @Override
        public void run() {
            DiskMessageStore.logger.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
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
