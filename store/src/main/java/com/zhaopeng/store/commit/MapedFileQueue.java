package com.zhaopeng.store.commit;

import com.zhaopeng.store.UtilAll;
import com.zhaopeng.store.service.AllocateMapedFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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


    public void checkSelf() {
        this.readWriteLock.readLock().lock();
        try {
            if (!this.mapedFiles.isEmpty()) {
                MapedFile first = this.mapedFiles.get(0);
                MapedFile last = this.mapedFiles.get(this.mapedFiles.size() - 1);

                int sizeCompute =
                        (int) ((last.getFileFromOffset() - first.getFileFromOffset()) / this.mapedFileSize) + 1;
                int sizeReal = this.mapedFiles.size();
                if (sizeCompute != sizeReal) {
                    log.error(
                            "[BUG]The mapedfile queue's data is damaged, {} mapedFileSize={} sizeCompute={} sizeReal={}\n{}", //
                            this.storePath,//
                            this.mapedFileSize,//
                            sizeCompute,//
                            sizeReal,//
                            this.mapedFiles.toString()//
                    );
                }
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public MapedFile getMapedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MapedFile mapedFile = (MapedFile) mfs[i];
            if (mapedFile.getLastModifiedTimestamp() >= timestamp) {
                return mapedFile;
            }
        }

        return (MapedFile) mfs[mfs.length - 1];
    }


    private Object[] copyMapedFiles(final int reservedMapedFiles) {
        Object[] mfs = null;

        try {
            this.readWriteLock.readLock().lock();
            if (this.mapedFiles.size() <= reservedMapedFiles) {
                return null;
            }

            mfs = this.mapedFiles.toArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        return mfs;
    }


    public void truncateDirtyFiles(long offset) {
        List<MapedFile> willRemoveFiles = new ArrayList<MapedFile>();

        for (MapedFile file : this.mapedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mapedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePostion((int) (offset % this.mapedFileSize));
                    file.setCommittedPosition((int) (offset % this.mapedFileSize));
                } else {

                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }


    private void deleteExpiredFile(List<MapedFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (MapedFile file : files) {
                    if (!this.mapedFiles.remove(file)) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }


    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mapedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }


                try {
                    MapedFile mapedFile = new MapedFile(file.getPath(), mapedFileSize);

                    mapedFile.setWrotePostion(this.mapedFileSize);
                    mapedFile.setCommittedPosition(this.mapedFileSize);
                    this.mapedFiles.add(mapedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }


    public long howMuchFallBehind() {
        if (this.mapedFiles.isEmpty())
            return 0;

        long committed = this.committedWhere;
        if (committed != 0) {
            MapedFile mapedFile = this.getLastMapedFile(0, false);
            if (mapedFile != null) {
                return (mapedFile.getFileFromOffset() + mapedFile.getWrotePostion()) - committed;
            }
        }

        return 0;
    }


    public MapedFile getLastMapedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MapedFile mapedFileLast = null;
        {
            this.readWriteLock.readLock().lock();
            if (this.mapedFiles.isEmpty()) {
                createOffset = startOffset - (startOffset % this.mapedFileSize);
            } else {
                mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
            }
            this.readWriteLock.readLock().unlock();
        }

        if (mapedFileLast != null && mapedFileLast.isFull()) {
            createOffset = mapedFileLast.getFileFromOffset() + this.mapedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath =
                    this.storePath + File.separator
                            + UtilAll.offset2FileName(createOffset + this.mapedFileSize);
            MapedFile mapedFile = null;

            if (this.allocateMapedFileService != null) {
               // mapedFile = this.allocateMapedFileService.putRequestAndReturnMapedFile(nextFilePath,nextNextFilePath, this.mapedFileSize);
            } else {
                try {
                    mapedFile = new MapedFile(nextFilePath, this.mapedFileSize);
                } catch (IOException e) {
                    log.error("create mapedfile exception", e);
                }
            }

            if (mapedFile != null) {
                this.readWriteLock.writeLock().lock();
                if (this.mapedFiles.isEmpty()) {
                 //   mapedFile.setFirstCreateInQueue(true);
                }
                this.mapedFiles.add(mapedFile);
                this.readWriteLock.writeLock().unlock();
            }

            return mapedFile;
        }

        return mapedFileLast;
    }

    public MapedFile getLastMapedFile() {
        return this.getLastMapedFile(0);
    }

    public MapedFile getLastMapedFile(final long startOffset) {
        return getLastMapedFile(startOffset, true);
    }

    public MapedFile getLastMapedFileWithLock() {
        MapedFile mapedFileLast = null;
        this.readWriteLock.readLock().lock();
        if (!this.mapedFiles.isEmpty()) {
            mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
        }
        this.readWriteLock.readLock().unlock();

        return mapedFileLast;
    }

    public boolean resetOffset(long offset) {
        this.readWriteLock.writeLock().lock();
        if (!this.mapedFiles.isEmpty()) {
            MapedFile mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
            long lastOffset = mapedFileLast.getFileFromOffset() +
                    mapedFileLast.getWrotePostion();
            long diff = lastOffset - offset;

            final int maxdiff = 1024 * 1024 * 1024 * 2;

            if (diff > maxdiff) return false;
        }

        for (int i = this.mapedFiles.size() - 1; i >= 0; i--) {
            MapedFile mapedFileLast = this.mapedFiles.get(i);

            if (offset >= mapedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mapedFileLast.getFileSize());
                mapedFileLast.setCommittedPosition(where);
                mapedFileLast.setWrotePostion(where);
                break;
            } else {
                this.mapedFiles.remove(mapedFileLast);
            }
        }
        this.readWriteLock.writeLock().unlock();
        return true;
    }

    public long getMinOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mapedFiles.isEmpty()) {
                return this.mapedFiles.get(0).getFileFromOffset();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return -1;
    }


    public long getMaxOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mapedFiles.isEmpty()) {
                int lastIndex = this.mapedFiles.size() - 1;
                MapedFile mapedFile = this.mapedFiles.get(lastIndex);
                return mapedFile.getFileFromOffset() + mapedFile.getWrotePostion();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return 0;
    }

    public void deleteLastMapedFile() {
        if (!this.mapedFiles.isEmpty()) {
            int lastIndex = this.mapedFiles.size() - 1;
            MapedFile mapedFile = this.mapedFiles.get(lastIndex);
          //  mapedFile.destroy(1000);
            this.mapedFiles.remove(mapedFile);
            log.info("on recover, destroy a logic maped file " + mapedFile.getFileName());
        }
    }

    public int deleteExpiredFileByTime(//
                                       final long expiredTime, //
                                       final int deleteFilesInterval, //
                                       final long intervalForcibly,//
                                       final boolean cleanImmediately//
    ) {


        return 0;
    }


    public int deleteExpiredFileByOffset(long offset, int unitSize) {


        return 0;
    }


    public boolean commit(final int flushLeastPages) {
        boolean result = true;
        MapedFile mapedFile = this.findMapedFileByOffset(this.committedWhere, true);
        if (mapedFile != null) {
            long tmpTimeStamp = mapedFile.getStoreTimestamp();
            int offset = mapedFile.commit(flushLeastPages);
            long where = mapedFile.getFileFromOffset() + offset;
            result = (where == this.committedWhere);
            this.committedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }


    public MapedFile findMapedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            this.readWriteLock.readLock().lock();
            MapedFile mapedFile = this.getFirstMapedFile();

            if (mapedFile != null) {
                int index =
                        (int) ((offset / this.mapedFileSize) - (mapedFile.getFileFromOffset() / this.mapedFileSize));
                if (index < 0 || index >= this.mapedFiles.size()) {
                    log.warn(
                            "findMapedFileByOffset offset not matched, request Offset: {}, index: {}, mapedFileSize: {}, mapedFiles count: {}",//
                            offset,//
                            index,//
                            this.mapedFileSize,//
                            this.mapedFiles.size()//
                    );
                }

                try {
                    return this.mapedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mapedFile;
                    }
                }
            }
        } catch (Exception e) {
            log.error("findMapedFileByOffset Exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }


    private MapedFile getFirstMapedFile() {
        if (this.mapedFiles.isEmpty()) {
            return null;
        }

        return this.mapedFiles.get(0);
    }


    public MapedFile getLastMapedFile2() {
        if (this.mapedFiles.isEmpty()) {
            return null;
        }
        return this.mapedFiles.get(this.mapedFiles.size() - 1);
    }


    public MapedFile findMapedFileByOffset(final long offset) {
        return findMapedFileByOffset(offset, false);
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {

        return false;
    }


    public MapedFile getFirstMapedFileOnLock() {
        try {
            this.readWriteLock.readLock().lock();
            return this.getFirstMapedFile();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public void shutdown(final long intervalForcibly) {
        this.readWriteLock.readLock().lock();
        for (MapedFile mf : this.mapedFiles) {
            // mf.shutdown(intervalForcibly);
        }
        this.readWriteLock.readLock().unlock();
    }


    public void destroy() {
        this.readWriteLock.writeLock().lock();
        for (MapedFile mf : this.mapedFiles) {
            // mf.destroy(1000 * 3);
        }
        this.mapedFiles.clear();
        this.committedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
        this.readWriteLock.writeLock().unlock();
    }


    public long getCommittedWhere() {
        return committedWhere;
    }


    public void setCommittedWhere(long committedWhere) {
        this.committedWhere = committedWhere;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public List<MapedFile> getMapedFiles() {
        return mapedFiles;
    }


    public int getMapedFileSize() {
        return mapedFileSize;
    }
}