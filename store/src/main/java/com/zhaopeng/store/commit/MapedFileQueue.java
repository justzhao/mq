package com.zhaopeng.store.commit;

import com.zhaopeng.store.service.AllocateMapedFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by zhaopeng on 2017/8/1.
 */
public class MapedFileQueue {

    private static final Logger log = LoggerFactory.getLogger(MapedFileQueue.class);
    private static final int DeleteFilesBatchMax = 10;

    private final String storePath;

    private final int mapedFileSize;

    private final List<MapedFile> mapedFiles = new ArrayList<MapedFile>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final AllocateMapedFileService allocateMapedFileService;

    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;


    public MapedFileQueue(final String storePath, int mapedFileSize,
                          AllocateMapedFileService allocateMapedFileService) {
        this.storePath = storePath;
        this.mapedFileSize = mapedFileSize;
        this.allocateMapedFileService = allocateMapedFileService;
    }
}
