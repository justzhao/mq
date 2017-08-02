package com.zhaopeng.store.commit;

import java.io.File;
import java.nio.channels.FileChannel;

/**
 * Created by zhaopeng on 2017/8/1.
 */
public class MapedFile {

    private final String fileName;

    private final long fileFromOffset;

    private final int fileSize;

    private final File file;

    private FileChannel fileChannel;


    public MapedFile(String fileName, long fileFromOffset, int fileSize, File file) {
        this.fileName = fileName;
        this.fileFromOffset = fileFromOffset;
        this.fileSize = fileSize;
        this.file = file;
    }
}
