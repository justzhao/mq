package com.zhaopeng.store.disk;

/**
 * Created by zhaopeng on 2017/8/13.
 */
public enum GetMessageStatus {
    FOUND,
    NO_MATCHED_MESSAGE,
    OFFSET_FOUND_NULL,
    OFFSET_OVERFLOW_BADLY,
    OFFSET_OVERFLOW_ONE,
    OFFSET_TOO_SMALL,
    NO_MATCHED_LOGIC_QUEUE,
    NO_MESSAGE_IN_QUEUE,
    
}
