package com.zhaopeng.store.commit;

import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.store.config.MessageStoreConfig;
import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.entity.PutMessageResult;
import com.zhaopeng.store.service.AllocateMapedFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


    public CommitLog(MessageStoreConfig config,AllocateMapedFileService allocateMapedFileService,
                     StoreCheckpoint storeCheckpoint) {
        this.messageStoreConfig = config;
        this.storeCheckpoint=storeCheckpoint;
        this.flushCommitLogService = new GroupCommitService();
        this.allocateMapedFileService=allocateMapedFileService;
        this.mapedFileQueue = new MapedFileQueue(messageStoreConfig.getStorePathCommitLog(),
                messageStoreConfig.getMapedFileSizeCommitLog(), this.allocateMapedFileService);
    }

    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {


        return null;
    }

    abstract class FlushCommitLogService extends ServiceThread {
    }

    class FlushRealTimeService extends FlushCommitLogService {
        private static final int RetryTimesOver = 3;
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;


        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                boolean flushCommitLogTimed = CommitLog.this.getMessageStoreConfig().isFlushCommitLogTimed();

                int interval = CommitLog.this.getMessageStoreConfig().getFlushIntervalCommitLog();
                int flushPhysicQueueLeastPages = CommitLog.this.getMessageStoreConfig().getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.getMessageStoreConfig().getFlushCommitLogThoroughInterval();


                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = ((printTimes++ % 10) == 0);
                }

                try {
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    CommitLog.this.mapedFileQueue.commit(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mapedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RetryTimesOver && !result; i++) {
                result = CommitLog.this.mapedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return FlushCommitLogService.class.getSimpleName();
        }


        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mapedFileQueue.howMuchFallBehind());
        }


        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
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

        @Override
        protected void onWaitEnd() {

        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }

        @Override
        public void run() {

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
}
