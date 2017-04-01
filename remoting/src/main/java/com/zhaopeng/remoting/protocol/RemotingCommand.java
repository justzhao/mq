package com.zhaopeng.remoting.protocol;

import java.nio.ByteBuffer;

/**
 * Created by zhaopeng on 2017/3/25.
 */
public class RemotingCommand {


    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND

    // body
    private transient byte[] body;

    // 业务类型
    private int code;

    //reqId
    private String requestId ;

    //req or resp
    private RemotingCommandType type;

    private boolean oneWay;

    String remark;


    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    /**
     * 根据body的数据长度产生包头数据
     * @param bodyLength
     * @return
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size , 包头和消息题的总数据长度
        int length = 4;

        // 2> header data length  包头数据编码
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;
        //分配一个4 字节+ 包头长度的缓存
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length  int 长度为4个字节
        result.putInt(length);

        // header length  包头的长度
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



    public static RemotingCommand createResponseCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        cmd.setType(RemotingCommandType.RESPONSE_COMMAND);
        return cmd;
    }



    public static int getRpcType() {
        return RPC_TYPE;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public RemotingCommandType getType() {
        return type;
    }

    public void setType(RemotingCommandType type) {
        this.type = type;
    }

    public boolean isOneWay() {
        return oneWay;
    }

    public void setOneWay(boolean oneWay) {
        this.oneWay = oneWay;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}
