package com.zhaopeng.store.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhaopeng on 2017/8/1.
 */
public class MapedFile {

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

    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;


    public MapedFile(String fileName, int fileSize) throws IOException {


        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
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




}
