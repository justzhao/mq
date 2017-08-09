package com.zhaopeng.store.commit;

import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.entity.enums.AppendMessageStatus;
import com.zhaopeng.store.util.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhaopeng on 2017/8/1.
 */
public class MapedFile {

    private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
    public static final int OS_PAGE_SIZE = 1024 * 4;
    private static final Logger log = LoggerFactory.getLogger(MapedFile.class);

    private static final AtomicLong TotalMapedVitualMemory = new AtomicLong(0);

    private static final AtomicInteger TotalMapedFiles = new AtomicInteger(0);

    protected final AtomicLong refCount = new AtomicLong(1);

    private final String fileName;

    private final long fileFromOffset;

    private final int fileSize;

    private final File file;

    private final MappedByteBuffer mappedByteBuffer;

    private final AtomicInteger wrotePostion = new AtomicInteger(0);

    private final AtomicInteger committedPosition = new AtomicInteger(0);

    private FileChannel fileChannel;

    private ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageUtil.MSG_ID_LENGTH);

    private  final  CommitLog commitLog;
    private volatile long storeTimestamp = 0;

    private boolean firstCreateInQueue = false;

    private final ByteBuffer msgStoreItemMemory;

    public MapedFile(String fileName, int fileSize, CommitLog commitLog, ByteBuffer msgStoreItemMemory) throws IOException {


        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.commitLog = commitLog;
        this.msgStoreItemMemory = msgStoreItemMemory;
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("new FileNotFoundException IOException {}", e);
            throw e;
        } catch (IOException e) {
            log.error("new MapedFile IOException {}", e);
            throw e;
        } finally {
            if (!ok) {
                if (!ok && this.fileChannel != null) {
                    this.fileChannel.close();
                }

            }
        }


    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg) {


        int currentPos = this.wrotePostion.get();


        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);
            this.wrotePostion.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        return null;

    }

    public void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static int getOsPageSize() {
        return OS_PAGE_SIZE;
    }

    public static Logger getLog() {
        return log;
    }

    public static AtomicLong getTotalMapedVitualMemory() {
        return TotalMapedVitualMemory;
    }

    public static AtomicInteger getTotalMapedFiles() {
        return TotalMapedFiles;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getFileSize() {
        return fileSize;
    }

    public File getFile() {
        return file;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public int getWrotePostion() {
        return wrotePostion.get();
    }

    public int getCommittedPosition() {
        return committedPosition.get();
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }


    public int commit(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = this.wrotePostion.get();
                this.mappedByteBuffer.force();
                this.committedPosition.set(value);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
                this.committedPosition.set(this.wrotePostion.get());
            }
        }

        return this.getCommittedPosition();
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePostion.get();


        if (this.isFull()) {
            return true;
        }


        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }


    public boolean isFull() {
        return this.fileSize == this.wrotePostion.get();
    }


    public synchronized boolean hold() {

        if (this.refCount.getAndIncrement() > 0) {
            return true;
        } else {
            this.refCount.getAndDecrement();
        }


        return false;
    }

    public void release() {
        long value = this.refCount.decrementAndGet();
        return;

    }


    public long getLastModifiedTimestamp() {
        return this.file.lastModified();

    }


    public void setWrotePostion(int pos) {
        this.wrotePostion.set(pos);
    }


    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }


    public String getMessageId(final MessageExtBrokerInner msg, long wroteOffset) {



        String msgId = MessageUtil.createMessageId(msgIdMemory, msg.getBornHostBytes(), wroteOffset);

        return msgId;
    }

    /**
     * @param fileFromOffset 文件的偏移量
     * @param byteBuffer     缓冲区
     * @param maxBlank       当前文件还可以写的长度
     * @param msg
     * @return
     */
    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBrokerInner msg) {


        MessageExtBrokerInner msgInner = msg;
        //  物理偏移量
        long wroteOffset = fileFromOffset + byteBuffer.position();

        String msgId = getMessageId(msg,wroteOffset);


        String key = msgInner.getTopic() + "-" + msgInner.getQueueId();
        Long queueOffset = commitLog.getTopicQueueTable().get(key);
        if (null == queueOffset) {
            queueOffset = 0L;
            commitLog.getTopicQueueTable().put(key, queueOffset);
        }




        final byte[] topicData = msgInner.getTopic().getBytes(MessageUtil.CHARSET_UTF8);
        final int topicLength = topicData == null ? 0 : topicData.length;

        final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

        final int msgLen = calMsgLength(bodyLength, topicLength);

        // Exceeds the maximum message
        if (msgLen > this.fileSize) {
          log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.fileSize);
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
            final long beginTimeMills =System.currentTimeMillis();
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
            return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                    queueOffset,System.currentTimeMillis()- beginTimeMills);
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
        this.msgStoreItemMemory.putInt(msgInner.getFlag());
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
        this.msgStoreItemMemory.put(msgInner.getBornHostBytes());
        // 13 RECONSUMETIMES
        this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
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


        return result;
    }

    private void resetMsgStoreItemMemory(final int length) {
        this.msgStoreItemMemory.flip();
        this.msgStoreItemMemory.limit(length);
    }


    private int calMsgLength(int bodyLength, int topicLength) {
        final int msgLen = 4 // 1 TOTALSIZE
                + 4 // 2 MAGICCODE
                + 4 // 3 BODYCRC
                + 4 // 4 QUEUEID
                + 4 // 5 FLAG
                + 8 // 6 QUEUEOFFSET
                + 8 // 7 PHYSICALOFFSET
              //  + 4 // 8 SYSFLAG
                + 8 // 9 BORNTIMESTAMP
                + 8 // 10 BORNHOST
                + 8 // 11 STORETIMESTAMP
                + 8 // 12 STOREHOSTADDRESS
                + 4 // 13 RECONSUMETIMES
              //  + 8 // 14 Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) // 14 BODY
                + 1 + topicLength // 15 TOPIC
                // propertiesLength
                + 0;
        return msgLen;
    }

}
