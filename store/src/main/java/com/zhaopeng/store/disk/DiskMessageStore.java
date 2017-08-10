package com.zhaopeng.store.disk;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.remoting.common.SystemClock;
import com.zhaopeng.store.ConsumeQueue;
import com.zhaopeng.store.MessageStore;
import com.zhaopeng.store.commit.CommitLog;
import com.zhaopeng.store.config.MessageStoreConfig;
import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.entity.PutMessageResult;
import com.zhaopeng.store.entity.enums.PutMessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    @Override
    public Message getMessage(PullMesageInfo pull) {
        return null;
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
        PutMessageResult result = this.commitLog.putMessage(msg);


        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 1000) {
            logger.info("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBodyLength());
        }


        return result;

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


}
