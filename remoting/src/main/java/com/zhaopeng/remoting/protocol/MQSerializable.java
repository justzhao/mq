package com.zhaopeng.remoting.protocol;

import com.google.common.base.Strings;

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

    /**
     * 需要被序列化的字段有如下
     * remark  , code（业务类型）, opaque(requestID)  ,flag, 扩展字段,
     *
     * @param cmd
     * @return
     */
    public static byte[] mqProtocolEncode(RemotingCommand cmd) {
        //  remark
        byte[] remarkBytes = null;
        int remarkLen = 0;
        if (!Strings.isNullOrEmpty(cmd.getRemark())) {
            remarkBytes = cmd.getRemark().getBytes(CHARSET_UTF8);
            remarkLen = remarkBytes.length;
        }
        // 扩展字段
        byte[] extFieldsBytes = null;
        int extLen = 0;
        if (cmd.getExtFields() != null && !cmd.getExtFields().isEmpty()) {
            extFieldsBytes = mapSerialize(cmd.getExtFields());
            extLen = extFieldsBytes.length;
        }

        // 计算头部总长度
        int totalLen = calTotalLen(remarkLen, extLen);

        //  一下是序列化的内容


        ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);
        // int code 转为short
        headerBuffer.putShort((short) cmd.getCode());

        // int opaque
        headerBuffer.putInt(cmd.getOpaque());
        // int flag
        headerBuffer.putInt(cmd.getFlag());
        // String remark
        if (remarkBytes != null) {
            // 放入mark字节的长度，和mark 的内容
            headerBuffer.putInt(remarkBytes.length);
            headerBuffer.put(remarkBytes);
        } else {
            headerBuffer.putInt(0);
        }
        // extFields 扩展字段
        if (extFieldsBytes != null) {
            headerBuffer.putInt(extFieldsBytes.length);
            headerBuffer.put(extFieldsBytes);
        } else {
            headerBuffer.putInt(0);

        }
        return headerBuffer.array();
    }

    public static RemotingCommand rocketMQProtocolDecode(final byte[] headerArray) {

        RemotingCommand cmd = new RemotingCommand();
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerArray);
        // int  code  业务类型字段
        cmd.setCode(headerBuffer.getShort());
        // int opaque 请求id
        cmd.setOpaque(headerBuffer.getInt());
        // int flag 标识位
        cmd.setFlag(headerBuffer.getInt());
        //  remark
        int remarkLength = headerBuffer.getInt();
        if (remarkLength > 0) {
            byte[] remarkContent = new byte[remarkLength];
            headerBuffer.get(remarkContent);
            cmd.setRemark(new String(remarkContent, CHARSET_UTF8));
        }
        // map  扩展字段
        int extFieldsLength = headerBuffer.getInt();
        if (extFieldsLength > 0) {
            byte[] extFieldsBytes = new byte[extFieldsLength];
            headerBuffer.get(extFieldsBytes);
            cmd.setExtFields(mapDeserialize(extFieldsBytes));
        }
        return cmd;
    }

    private static int calTotalLen(int remark, int ext) {
        // int code(被转化成short)
        int length = 2
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

    /**
     * 序列化map到bytes数组
     * @param map
     * @return
     */
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
                        2 + entry.getKey().getBytes(CHARSET_UTF8).length
                                // valSize + val
                                + 4 + entry.getValue().getBytes(CHARSET_UTF8).length;
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
                key = entry.getKey().getBytes(CHARSET_UTF8);
                val = entry.getValue().getBytes(CHARSET_UTF8);

                content.putShort((short) key.length);
                content.put(key);

                content.putInt(val.length);
                content.put(val);
            }
        }

        return content.array();
    }

    /**
     * 反序列化 bytes数组到map
     * @param bytes
     * @return
     */
    public static HashMap<String, String> mapDeserialize(byte[] bytes) {
        if (bytes == null || bytes.length <= 0)
            return null;

        HashMap<String, String> map = new HashMap<>();
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

            map.put(new String(keyContent, CHARSET_UTF8), new String(valContent,
                    CHARSET_UTF8));
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
