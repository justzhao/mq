package com.zhaopeng.store.entity.enums;

/**
 * Created by zhaopeng on 2017/7/31.
 */
public enum PutMessageStatus {

    PUT_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    SERVICE_NOT_AVAILABLE,
    CREATE_MAPEDFILE_FAILED,
    MESSAGE_ILLEGAL,
    PROPERTIES_SIZE_EXCEEDED,
    OS_PAGECACHE_BUSY,
    UNKNOWN_ERROR,

}
