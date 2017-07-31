package com.zhaopeng.store.config;

import java.io.File;

/**
 * Created by zhaopeng on 2017/7/30.
 */
public class MessageStoreConfig {


    private String storePathRootDir = System.getProperty("user.home") + File.separator + "mqstore";


    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "mqstore"
            + File.separator + "commitlog";

    private long osPageCacheBusyTimeOutMills = 1000;

    public long getOsPageCacheBusyTimeOutMills() {
        return osPageCacheBusyTimeOutMills;
    }
}
