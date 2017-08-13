package com.zhaopeng.store;

import com.zhaopeng.store.commit.MapedFileQueue;
import com.zhaopeng.store.entity.enums.DiskMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Created by zhaopeng on 2017/7/30.
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

        this.mapedFileQueue = new MapedFileQueue(queueDir, mapedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQStoreUnitSize);
    }

}


