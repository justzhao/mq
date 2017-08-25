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

    public static final int CQStoreUnitSize = 12;
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

    public boolean load() {
        boolean result = this.mapedFileQueue.load();
        logger.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }


    public void putMessagePostionInfoWrapper(long offset, int size, long storeTimestamp,
                                             long logicOffset) {
        final int MaxRetries = 30;

        for (int i = 0; i < MaxRetries ; i++) {
            boolean result = this.putMessagePostionInfo(offset, size, logicOffset);
            if (result) {

                return;
            }

            else {

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("putMessagePostionInfoWrapper error {}", e);
                }
            }
        }

        // XXX: warn and notify me
        logger.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);

    }


    private boolean putMessagePostionInfo(final long offset, final int size,
                                          final long cqOffset) {

        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQStoreUnitSize);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
//        this.byteBufferIndex.putLong(tagsCode);

        final long expectLogicOffset = cqOffset * CQStoreUnitSize;

        MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile(expectLogicOffset);
        if (mapedFile != null) {

            if (mapedFile.isFirstCreateInQueue() && cqOffset != 0 && mapedFile.getWrotePostion() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.fillPreBlank(mapedFile, expectLogicOffset);
                logger.info("fill pre blank space " + mapedFile.getFileName() + " " + expectLogicOffset + " "
                        + mapedFile.getWrotePostion());
            }

            if (cqOffset != 0) {
                long currentLogicOffset = mapedFile.getWrotePostion() + mapedFile.getFileFromOffset();
                if (expectLogicOffset != currentLogicOffset) {
                    logger
                            .warn(
                                    "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",//
                                    expectLogicOffset, //
                                    currentLogicOffset,//
                                    this.topic,//
                                    this.queueId,//
                                    expectLogicOffset - currentLogicOffset//
                            );
                }
            }
            this.maxPhysicOffset = offset;
            return mapedFile.appendMessage(this.byteBufferIndex.array());
        }

        return false;
    }



    private void fillPreBlank(final MapedFile mapedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQStoreUnitSize);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mapedFileQueue.getMapedFileSize());
        for (int i = 0; i < until; i += CQStoreUnitSize) {
            mapedFile.appendMessage(byteBuffer.array());
        }
    }


}


