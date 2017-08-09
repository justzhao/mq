package com.zhaopeng.store.commit;

import com.zhaopeng.store.util.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zhaopeng on 2017/8/6.
 */
public class StoreCheckpoint {
    private static final Logger log = LoggerFactory.getLogger(StoreCheckpoint.class);
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private volatile long physicMsgTimestamp = 0;
    private volatile long logicsMsgTimestamp = 0;
    private volatile long indexMsgTimestamp = 0;


    public StoreCheckpoint(final String scpPath) throws IOException {
        File file = new File(scpPath);
       // MapedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MapedFile.OS_PAGE_SIZE);

        if (fileExists) {
            log.info("store checkpoint file exists, " + scpPath);
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0);
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8);
            this.indexMsgTimestamp = this.mappedByteBuffer.getLong(16);

            log.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", "
                    + UtilAll.timeMillisToHumanString(this.physicMsgTimestamp));
            log.info("store checkpoint file logicsMsgTimestamp " + this.logicsMsgTimestamp + ", "
                    + UtilAll.timeMillisToHumanString(this.logicsMsgTimestamp));
            log.info("store checkpoint file indexMsgTimestamp " + this.indexMsgTimestamp + ", "
                    + UtilAll.timeMillisToHumanString(this.indexMsgTimestamp));
        } else {
            log.info("store checkpoint file not exists, " + scpPath);
        }
    }


    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
    //    MapedFile.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void flush() {
        this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
        this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
        this.mappedByteBuffer.putLong(16, this.indexMsgTimestamp);
        this.mappedByteBuffer.force();
    }


    public long getPhysicMsgTimestamp() {
        return physicMsgTimestamp;
    }


    public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
        this.physicMsgTimestamp = physicMsgTimestamp;
    }
}
