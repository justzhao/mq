package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by zhaopeng on 2017/3/25.
 */
public class NettyDecoder extends MessageToByteEncoder<RemotingCommand> {
    protected void encode(ChannelHandlerContext ctx, RemotingCommand msg, ByteBuf out) throws Exception {

    }
}
