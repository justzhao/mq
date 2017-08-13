package com.zhaopeng.store.config;

import java.io.File;

/**
 * Created by zhaopeng on 2017/8/13.
 */
public class StorePathConfigHelper {
    public static String getStorePathConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue";
    }


    public static String getStorePathIndex(final String rootDir) {
        return rootDir + File.separator + "index";
    }


    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }


    public static String getAbortFile(final String rootDir) {
        return rootDir + File.separator + "abort";
    }


    public static String getDelayOffsetStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "delayOffset.json";
    }


    public static String getTranStateTableStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "statetable";
    }


    public static String getTranRedoLogStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "redolog";
    }
}
