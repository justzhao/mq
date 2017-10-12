
package com.zhaopeng.mq.store;

public enum ReadOffsetType {
    /**
     * From memory
     */
    READ_FROM_MEMORY,
    /**
     * From storage
     */
    READ_FROM_STORE,
    /**
     * From memory,then from storage
     */
    MEMORY_FIRST_THEN_STORE;
}
