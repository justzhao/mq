package com.zhaopeng.store.commit;

import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.store.config.MessageStoreConfig;
import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.entity.PutMessageResult;
import com.zhaopeng.store.entity.enums.PutMessageStatus;
import com.zhaopeng.store.service.AllocateMapedFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaopeng on 2017/7/27.
 */
public class CommitLog {

    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);
    private volatile long confirmOffset = -1L;
    private volatile long beginTimeInLock = 0;

    private final MapedFileQueue mapedFileQueue;

    private final MessageStoreConfig messageStoreConfig;

    private final FlushCommitLogService flushCommitLogService;

    private final AllocateMapedFileService allocateMapedFileService;

    private final StoreCheckpoint storeCheckpoint;

    public CommitLog() throws IOException {

        this.messageStoreConfig = new MessageStoreConfig();
        this.storeCheckpoint = new StoreCheckpoint("c://defaut");
        this.flushCommitLogService = new GroupCommitService();
        this.allocateMapedFileService = new AllocateMapedFileService();
        this.mapedFileQueue = new MapedFileQueue(messageStoreConfig.getStorePathCommitLog(),
                messageStoreConfig.getMapedFileSizeCommitLog(), this.allocateMapedFileService);
    }


    public CommitLog(MessageStoreConfig config, AllocateMapedFileService allocateMapedFileService,
                     StoreCheckpoint storeCheckpoint) {
        this.messageStoreConfig = config;
        this.storeCheckpoint = storeCheckpoint;
        this.flushCommitLogService = new GroupCommitService();
        this.allocateMapedFileService = allocateMapedFileService;
        this.mapedFileQueue = new MapedFileQueue(messageStoreConfig.getStorePathCommitLog(),
                messageStoreConfig.getMapedFileSizeCommitLog(), this.allocateMapedFileService);
    }

    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {

        long eclipseTimeInLock = 0;
        MapedFile unlockMapedFile = null;
        MapedFile mapedFile = this.mapedFileQueue.getLastMapedFileWithLock();
        AppendMessageResult result = null;
        synchronized (this) {
            long beginLockTimestamp = System.currentTimeMillis();
            this.beginTimeInLock = beginLockTimestamp;


           // msg.setStoreTimestamp(beginLockTimestamp);

            if (null == mapedFile || mapedFile.isFull()) {
                mapedFile = this.mapedFileQueue.getLastMapedFile();
            }
            if (null == mapedFile) {
               log.error("create maped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getHost());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED);
            }
            result = mapedFile.appendMessage(msg);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMapedFile = mapedFile;
                    // Create a new file, re-write the message
                    mapedFile = this.mapedFileQueue.getLastMapedFile();
                    if (null == mapedFile) {
                        // XXX: warn and notify me
                        log.error("create maped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getHost());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED);
                    }
                    result = mapedFile.appendMessage(msg);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR );
            }

            eclipseTimeInLock = System.currentTimeMillis() - beginLockTimestamp;
            beginTimeInLock = 0;
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK);
        return putMessageResult;
    }

    abstract class FlushCommitLogService extends ServiceThread {
    }


    public static class GroupCommitRequest {
        private final long nextOffset;
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile boolean flushOK = false;


        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }


        public long getNextOffset() {
            return nextOffset;
        }


        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }


        public boolean waitForFlush(long timeout) {
            try {
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return this.flushOK;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    class GroupCommitService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();

        public void putRequest(final GroupCommitRequest request) {
            synchronized (this) {
                this.requestsWrite.add(request);
                if (!this.hasNotified) {
                    this.hasNotified = true;
                    this.notify();
                }
            }
        }


        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }


        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {

                    boolean flushOK = false;
                    for (int i = 0; (i < 2) && !flushOK; i++) {
                        flushOK = (CommitLog.this.mapedFileQueue.getCommittedWhere() >= req.getNextOffset());

                        if (!flushOK) {
                            CommitLog.this.mapedFileQueue.commit(0);
                        }
                    }

                    req.wakeupCustomer(flushOK);
                }

                long storeTimestamp = CommitLog.this.mapedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead.clear();
            } else {

                CommitLog.this.mapedFileQueue.commit(0);
            }
        }


        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(0);
                    this.doCommit();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            log.info(this.getServiceName() + " service end");
        }


        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }


        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public MapedFileQueue getMapedFileQueue() {
        return mapedFileQueue;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public FlushCommitLogService getFlushCommitLogService() {
        return flushCommitLogService;
    }

    public AllocateMapedFileService getAllocateMapedFileService() {
        return allocateMapedFileService;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }


    public long getConfirmOffset() {
        return confirmOffset;
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    public void setBeginTimeInLock(long beginTimeInLock) {
        this.beginTimeInLock = beginTimeInLock;
    }

}
