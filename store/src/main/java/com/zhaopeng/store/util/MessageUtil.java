package com.zhaopeng.store.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by zhaopeng on 2017/8/9.
 */
public class MessageUtil {

    public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static int MSG_ID_LENGTH = 16;

    public static String createMessageId(final ByteBuffer input, final ByteBuffer addr, final long offset) {
        input.flip();
        input.limit(MSG_ID_LENGTH);

        input.put(addr);
        input.putLong(offset);

        return UtilAll.bytes2string(input.array());
    }
}
