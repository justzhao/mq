package com.zhaopeng.remoting.protocol;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaopeng on 2017/3/25.
 */
public class RemotingCommand {


    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND

    // body
    private transient byte[] body;

    //
    private int code;

    //reqId
    private static AtomicInteger requestId = new AtomicInteger(0);







    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    /**

     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length));

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }

    public static byte[] markProtocolType(int source) {
        byte[] result = new byte[4];

        result[0] = (byte)source;
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }

    private byte[] headerEncode() {
       return RemotingSerializable.encode(this);
    }


}
