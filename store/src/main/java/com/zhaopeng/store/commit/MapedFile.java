package com.zhaopeng.store.commit;

import com.zhaopeng.store.disk.SelectMapedBufferResult;
import com.zhaopeng.store.entity.MessageExtBrokerInner;
import com.zhaopeng.store.util.MessageUtil;
import com.zhaopeng.store.util.UtilAll;
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
public class MapedFile extends ReferenceResource {

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

    //  private  final  CommitLog commitLog;
    private volatile long storeTimestamp = 0;

    private boolean firstCreateInQueue = false;

    //  private final ByteBuffer msgStoreItemMemory;

    public MapedFile(String fileName, int fileSize) throws IOException {


        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        //   this.commitLog = commitLog;
        // this.msgStoreItemMemory = msgStoreItemMemory;
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


    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePostion() + " M:"
                        + this.getCommittedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy maped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }




    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, AppendMessageCallback callback) {
        int currentPos = this.wrotePostion.get();
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = callback.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);
            this.wrotePostion.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        return null;

    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePostion.get();


        if ((currentPos + data.length) <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            byteBuffer.put(data);
            this.wrotePostion.addAndGet(data.length);
            return true;
        }

        return false;
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

    @Override
    public boolean cleanup(long currentRef) {
        return false;
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


    public SelectMapedBufferResult selectMapedBuffer(int pos) {
        if (pos < this.wrotePostion.get() && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = this.wrotePostion.get() - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }


    public SelectMapedBufferResult selectMapedBuffer(int pos, int size) {
        if ((pos + size) <= this.wrotePostion.get()) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        }

        else {
            log.warn("selectMapedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }


        return null;
    }

    @Override
    public String toString() {
        return "MapedFile{" +
                "refCount=" + refCount +
                ", fileName='" + fileName + '\'' +
                ", fileFromOffset=" + fileFromOffset +
                ", fileSize=" + fileSize +
                ", file=" + file +
                ", mappedByteBuffer=" + mappedByteBuffer +
                ", wrotePostion=" + wrotePostion +
                ", committedPosition=" + committedPosition +
                ", fileChannel=" + fileChannel +
                ", msgIdMemory=" + msgIdMemory +
                ", storeTimestamp=" + storeTimestamp +
                ", firstCreateInQueue=" + firstCreateInQueue +
                '}';
    }
}
