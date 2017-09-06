package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by zhaopeng on 2017/3/25.
 */
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    private static final Logger logger = LoggerFactory.getLogger(NettyEncoder.class);

    protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {

        try {
            // 包头数据 包括 总长度，包头长度，包头数据
            ByteBuffer header = remotingCommand.encodeHeader();
            out.writeBytes(header);
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            logger.error("encode exception {}", e);
            if (remotingCommand != null) {
                logger.error(remotingCommand.toString());
            }
            ctx.close();
        }
    }
}
