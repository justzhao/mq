package com.zhaopeng.remoting.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by zhaopeng on 2017/4/3.
 */
public class MQSerializable {

    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static byte[] mqProtocolEncode(RemotingCommand cmd) {
        // String remark
        byte[] remarkBytes = null;
        int remarkLen = 0;
        if (cmd.getRemark() != null && cmd.getRemark().length() > 0) {
            remarkBytes = cmd.getRemark().getBytes(JsonSerializable.CHARSET_UTF8);
            remarkLen = remarkBytes.length;
        }

        int extLen = 0;


        // ################### cal total length
        int totalLen = calTotalLen(remarkLen, extLen);

        // ################### content
        ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);
        // int code(~32767)
        headerBuffer.putShort((short) cmd.getCode());
        // LanguageCode language
      //  headerBuffer.put(cmd.getLanguage().getCode());
        // int version(~32767)
      //  headerBuffer.putShort((short) cmd.getVersion());
        // int opaque
        headerBuffer.put(cmd.getRequestId().getBytes(JsonSerializable.CHARSET_UTF8));
        // int flag
       // headerBuffer.putInt(cmd.);
        // String remark
        if (remarkBytes != null) {
            headerBuffer.putInt(remarkBytes.length);
            headerBuffer.put(remarkBytes);
        } else {
            headerBuffer.putInt(0);
        }
        // HashMap<String, String> extFields;

            headerBuffer.putInt(0);


        return headerBuffer.array();
    }

    public static byte[] mapSerialize(HashMap<String, String> map) {
        // keySize+key+valSize+val
        // keySize+key+valSize+val
        if (null == map || map.isEmpty())
            return null;

        int totalLength = 0;
        int kvLength;
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                kvLength =
                        // keySize + Key
                        2 + entry.getKey().getBytes(JsonSerializable.CHARSET_UTF8).length
                                // valSize + val
                                + 4 + entry.getValue().getBytes(JsonSerializable.CHARSET_UTF8).length;
                totalLength += kvLength;
            }
        }

        ByteBuffer content = ByteBuffer.allocate(totalLength);
        byte[] key;
        byte[] val;
        it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                key = entry.getKey().getBytes(JsonSerializable.CHARSET_UTF8);
                val = entry.getValue().getBytes(JsonSerializable.CHARSET_UTF8);

                content.putShort((short) key.length);
                content.put(key);

                content.putInt(val.length);
                content.put(val);
            }
        }

        return content.array();
    }

    private static int calTotalLen(int remark, int ext) {
        // int code(~32767)
        int length = 2
                // LanguageCode language
                + 1
                // int version(~32767)
                + 2
                // int opaque
                + 4
                // int flag
                + 4
                // String remark
                + 4 + remark
                // HashMap<String, String> extFields
                + 4 + ext;

        return length;
    }

    public static RemotingCommand rocketMQProtocolDecode(final byte[] headerArray) {
        RemotingCommand cmd = new RemotingCommand();

        return cmd;
    }

    public static HashMap<String, String> mapDeserialize(byte[] bytes) {
        if (bytes == null || bytes.length <= 0)
            return null;

        HashMap<String, String> map = new HashMap<String, String>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        short keySize = 0;
        byte[] keyContent = null;
        int valSize = 0;
        byte[] valContent = null;
        while (byteBuffer.hasRemaining()) {
            keySize = byteBuffer.getShort();
            keyContent = new byte[keySize];
            byteBuffer.get(keyContent);

            valSize = byteBuffer.getInt();
            valContent = new byte[valSize];
            byteBuffer.get(valContent);

            map.put(new String(keyContent, JsonSerializable.CHARSET_UTF8), new String(valContent,
                    JsonSerializable.CHARSET_UTF8));
        }
        return map;
    }


    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
