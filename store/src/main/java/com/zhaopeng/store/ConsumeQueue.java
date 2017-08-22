package com.zhaopeng.store;

import com.zhaopeng.store.commit.MapedFile;
import com.zhaopeng.store.commit.MapedFileQueue;
import com.zhaopeng.store.disk.SelectMapedBufferResult;
import com.zhaopeng.store.disk.DiskMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Created by zhaopeng on 2017/7/30.
 * consumerQueue 会保存消息的offset和长度
 */

public class ConsumeQueue {

    public static final int CQStoreUnitSize = 20;
    private static final Logger logger = LoggerFactory.getLogger(ConsumeQueue.class);


    private final DiskMessageStore defaultMessageStore;

    private final MapedFileQueue mapedFileQueue;
    private final String topic;
    private final int queueId;
    private final ByteBuffer byteBufferIndex;

    private final String storePath;
    private final int mapedFileSize;
    private long maxPhysicOffset = -1;
    private volatile long minLogicOffset = 0;


    public ConsumeQueue(//
                        final String topic,//
                        final int queueId,//
                        final String storePath,//
                        final int mapedFileSize,//
                        final DiskMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mapedFileSize = mapedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath//
                + File.separator + topic//
                + File.separator + queueId;//

        this.mapedFileQueue = new MapedFileQueue(queueDir, mapedFileSize);

        this.byteBufferIndex = ByteBuffer.allocate(CQStoreUnitSize);
    }

    public static int getCQStoreUnitSize() {
        return CQStoreUnitSize;
    }

    public static Logger getLogger() {
        return logger;
    }

    public DiskMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public MapedFileQueue getMapedFileQueue() {
        return mapedFileQueue;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public ByteBuffer getByteBufferIndex() {
        return byteBufferIndex;
    }

    public String getStorePath() {
        return storePath;
    }

    public int getMapedFileSize() {
        return mapedFileSize;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long getMaxOffsetInQuque() {
        return this.mapedFileQueue.getMaxOffset() / CQStoreUnitSize;
    }

    public long getMinOffsetInQuque() {
        return this.minLogicOffset / CQStoreUnitSize;
    }

    public SelectMapedBufferResult getIndexBuffer(final long startIndex) {
        int mapedFileSize = this.mapedFileSize;
        long offset = startIndex * CQStoreUnitSize;
        if (offset >= this.getMinLogicOffset()) {
            MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset);
            if (mapedFile != null) {
                SelectMapedBufferResult result = mapedFile.selectMapedBuffer((int) (offset % mapedFileSize));
                return result;
            }
        }
        return null;
    }

    public long rollNextFile(final long index) {
        int mapedFileSize = this.mapedFileSize;
        int totalUnitsInFile = mapedFileSize / CQStoreUnitSize;
        return (index + totalUnitsInFile - index % totalUnitsInFile);
    }
}


