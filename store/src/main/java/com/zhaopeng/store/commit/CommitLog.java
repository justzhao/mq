package com.zhaopeng.store.commit;

import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.store.config.MessageStoreConfig;
import com.zhaopeng.store.disk.DiskMessageStore;
import com.zhaopeng.store.disk.SelectMapedBufferResult;
import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.entity.PutMessageResult;
import com.zhaopeng.store.entity.QueueRequest;
import com.zhaopeng.store.entity.enums.AppendMessageStatus;
import com.zhaopeng.store.entity.enums.PutMessageStatus;
import com.zhaopeng.store.util.MessageUtil;
import com.zhaopeng.store.util.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.zhaopeng.store.util.MessageUtil.MSG_ID_LENGTH;

/**
 * Created by zhaopeng on 2017/7/27.
 */
public class CommitLog {
    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);
    public final static int MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8;
    public final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;
    private volatile long confirmOffset = -1L;
    private volatile long beginTimeInLock = 0;
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);
    private final MapedFileQueue mapedFileQueue;
    private final MessageStoreConfig messageStoreConfig;
    private final FlushCommitLogService flushCommitLogService;
    //private final AllocateMapedFileService allocateMapedFileService;
    private final StoreCheckpoint storeCheckpoint;
    private final AppendMessageCallback appendMessageCallback;
    private final DiskMessageStore defaultMessageStore;
    public CommitLog(DiskMessageStore messageStore) throws IOException {

        this.defaultMessageStore=messageStore;
        this.messageStoreConfig = new MessageStoreConfig();
        this.storeCheckpoint = new StoreCheckpoint("c://defaut");
        this.flushCommitLogService = new GroupCommitService();
       // this.allocateMapedFileService = new AllocateMapedFileService();
        this.mapedFileQueue = new MapedFileQueue(messageStoreConfig.getStorePathCommitLog(),
                messageStoreConfig.getMapedFileSizeCommitLog());
        this.appendMessageCallback = new DefaultAppendMessageCallback(messageStoreConfig.getMaxMessageSize());
    }
    public CommitLog(MessageStoreConfig config,
                     StoreCheckpoint storeCheckpoint,DiskMessageStore messageStore) {
        this.messageStoreConfig = config;
        this.storeCheckpoint = storeCheckpoint;
        this.flushCommitLogService = new GroupCommitService();
        this.defaultMessageStore=messageStore;

        this.mapedFileQueue = new MapedFileQueue(messageStoreConfig.getStorePathCommitLog(),
                messageStoreConfig.getMapedFileSizeCommitLog());
        this.appendMessageCallback = new DefaultAppendMessageCallback(messageStoreConfig.getMaxMessageSize());
    }


    public SelectMapedBufferResult getMessage(final long offset, final int size) {
        int mapedFileSize = this.messageStoreConfig.getMapedFileSizeCommitLog();
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset, (0 == offset ? true : false));
        if (mapedFile != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(pos, size);
            return result;
        }

        return null;
    }
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        msg.setStoreTimestamp(System.currentTimeMillis());
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        AppendMessageResult result = null;

        MapedFile unlockMapedFile = null;
        MapedFile mapedFile = this.mapedFileQueue.getLastMapedFileWithLock();
        synchronized (this) {
            long beginLockTimestamp = System.currentTimeMillis();
            this.beginTimeInLock = beginLockTimestamp;
            msg.setStoreTimestamp(beginLockTimestamp);
            if (null == mapedFile || mapedFile.isFull()) {
                mapedFile = this.mapedFileQueue.getLastMapedFile();
            }
            if (null == mapedFile) {
                log.error("create maped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getHost());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED);
            }
            result = mapedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMapedFile = mapedFile;
                    // 创建一个新的文件
                    mapedFile = this.mapedFileQueue.getLastMapedFile();
                    if (null == mapedFile) {
                        log.error("create maped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getHost());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED);
                    }
                    result = mapedFile.appendMessage(msg, this.appendMessageCallback);
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
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR);
            }


            beginTimeInLock = 0;
        }

        GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
        if (msg.isWaitStoreMsgOK()) {
            GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
            service.putRequest(request);
            boolean flushOK = request.waitForFlush(messageStoreConfig.getSyncFlushTimeout());
            if (!flushOK) {
                log.error("do groupcommit, wait for flush failed, topic: " + msg.getTopic()
                        + " client address: " + msg.getHost());
                PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                return putMessageResult;
            }
        } else {
            service.wakeup();
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK);
        return putMessageResult;
    }


    public int deleteExpiredFile(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, //
                                 final boolean cleanImmediately) {
        return this.mapedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
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

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {

        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        private final int maxMessageSize;


        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        private void resetMsgStoreItemMemory(final int length) {
            this.msgStoreItemMemory.flip();
            this.msgStoreItemMemory.limit(length);
        }


        public String getMessageId(final MessageExtBrokerInner msg, long wroteOffset) {

            String msgId = MessageUtil.createMessageId(msgIdMemory, msg.getBornHostBytes(), wroteOffset);

            return msgId;
        }


        private int calMsgLength(int bodyLength, int topicLength) {
            final int msgLen = 4 // 1 TOTALSIZE
                    + 4 // 2 MAGICCODE
                    + 4 // 3 BODYCRC
                    + 4 // 4 QUEUEID
                  //  + 4 // 5 FLAG
                    + 8 // 6 QUEUEOFFSET
                    + 8 // 7 PHYSICALOFFSET
                    //  + 4 // 8 SYSFLAG
                    + 8 // 9 BORNTIMESTAMP
                    + 8 // 10 BORNHOST
                    + 8 // 11 STORETIMESTAMP
                    //+ 8 // 12 STOREHOSTADDRESS
                    //+ 4 // 13 RECONSUMETIMES
                    //  + 8 // 14 Prepared Transaction Offset
                    + 4 + (bodyLength > 0 ? bodyLength : 0) // 14 BODY
                    + 1 + topicLength // 15 TOPIC
                    // propertiesLength
                    + 0;
            return msgLen;
        }

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBrokerInner msg) {
            MessageExtBrokerInner msgInner = msg;
            //  物理偏移量
            long wroteOffset = fileFromOffset + byteBuffer.position();
            String msgId = getMessageId(msg, wroteOffset);
            String key = msgInner.getTopic() + "-" + msgInner.getQueueId();
            Long queueOffset = CommitLog.this.getTopicQueueTable().get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.getTopicQueueTable().put(key, queueOffset);
            }

            final byte[] topicData = msgInner.getTopic().getBytes(MessageUtil.CHARSET_UTF8);
            final int topicLength = topicData == null ? 0 : topicData.length;
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;
            final int msgLen = calMsgLength(bodyLength, topicLength);

            if (msgLen > this.maxMessageSize) {
                log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetMsgStoreItemMemory(maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BlankMagicCode);
                // 3 The remaining space may be any value
                //
                final long beginTimeMills = System.currentTimeMillis();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                        queueOffset, System.currentTimeMillis() - beginTimeMills);
            }
            // Initialization of storage space
            this.resetMsgStoreItemMemory(msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.MessageMagicCode);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
          //  this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG
            //  this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes());
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
         //   this.msgStoreItemMemory.put(msgInner.getBornHostBytes());
            // 13 RECONSUMETIMES
       //     this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            //  this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES


            final long beginTimeMills = System.currentTimeMillis();

            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                    msgInner.getStoreTimestamp(), queueOffset, System.currentTimeMillis() - beginTimeMills);


            /**
             * 更新MessageQueue
             */
            QueueRequest request=new QueueRequest(msgInner.getTopic(),msgInner.getQueueId(),fileFromOffset + byteBuffer.position()
                    ,msgLen,System.currentTimeMillis(),queueOffset);

            defaultMessageStore.doAddConsumeQueueRequest(request);
            CommitLog.this.topicQueueTable.put(key, ++queueOffset);

            return result;

        }
    }

    public long getMaxOffset() {
        return this.mapedFileQueue.getMaxOffset();
    }


    public SelectMapedBufferResult getData(final long offset) {
        return this.getData(offset, (0 == offset ? true : false));
    }


    public SelectMapedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mapedFileSize = this.messageStoreConfig.getMapedFileSizeCommitLog();
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset, returnFirstOnNotFound);
        if (mapedFile != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(pos);
            return result;
        }

        return null;
    }

    public boolean load() {
        boolean result = this.mapedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }


    public long getMinOffset() {
        MapedFile mapedFile = this.mapedFileQueue.getFirstMapedFileOnLock();
        if (mapedFile != null) {
            if (mapedFile.isAvailable()) {
                return mapedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mapedFile.getFileFromOffset());
            }
        }

        return -1;
    }
    public long rollNextFile(final long offset) {
        int mapedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return (offset + mapedFileSize - offset % mapedFileSize);
    }

    public void recoverNormally() {

        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mapedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MapedFile mapedFile = mapedFiles.get(index);
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            long processOffset = mapedFile.getFileFromOffset();
            long mapedFileOffset = 0;
            while (true) {
                QueueRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer,true);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    mapedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last maped file " + mapedFile.getFileName());
                        break;
                    } else {
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next physics file, " + mapedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mapedFile.getFileName());
                    break;
                }
            }

            processOffset += mapedFileOffset;
            this.mapedFileQueue.setCommittedWhere(processOffset);
            this.mapedFileQueue.truncateDirtyFiles(processOffset);
        }
    }


    public QueueRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer,  final boolean readBody) {

        return null;
    }



}
