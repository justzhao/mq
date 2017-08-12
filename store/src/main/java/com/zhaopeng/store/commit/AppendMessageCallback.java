package com.zhaopeng.store.commit;

import com.zhaopeng.store.entity.MessageExtBrokerInner;

import java.nio.ByteBuffer;

/**
 * Created by zhaopeng on 2017/8/12.
 */
public interface AppendMessageCallback {



    /**消息被序列化之后，写入缓冲区
     * @param fileFromOffset 文件的偏移量
     * @param byteBuffer     缓冲区
     * @param maxBlank       当前文件还可以写的长度
     * @param msg
     * @return
     */
    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                                        final int maxBlank, final MessageExtBrokerInner msg);
}
