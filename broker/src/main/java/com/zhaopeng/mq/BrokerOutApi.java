package com.zhaopeng.mq;

import com.zhaopeng.common.protocol.RequestCode;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.RegisterBrokerInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerResult;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.protocol.JsonSerializable;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by zhaopeng on 2017/6/20.
 */
public class BrokerOutApi {

    private static final Logger logger = LoggerFactory.getLogger(BrokerController.class);


    private final NettyClient nettyClient;

    public BrokerOutApi(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }


    public RegisterBrokerResult registerBrokerAll(//
                                                  final String groupName, // 1
                                                  final String brokerAddr, // 2
                                                  final String brokerName, // 3
                                                  final long brokerId,
                                                  final boolean oneway,// 4
                                                  final int timeoutMills// 5
    ) {


        RegisterBrokerResult registerBrokerResult = null;

        List<String> nameServerAddressList = this.nettyClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    RegisterBrokerResult result = this.registerBroker(namesrvAddr, groupName, brokerAddr, brokerName, brokerId, oneway, timeoutMills);
                    if (result != null) {
                        registerBrokerResult = result;
                    }

                    logger.info("register broker to name server {} OK", namesrvAddr);
                } catch (Exception e) {
                    logger.warn("registerBroker Exception, " + namesrvAddr, e);
                }
            }
        }
        return registerBrokerResult;

    }

    private RegisterBrokerResult registerBroker(final String namesrvAddr,
                                                final String group, // 1
                                                final String brokerAddr, // 2
                                                final String brokerName, // 3
                                                final long brokerId,
                                                final boolean oneway,// 4
                                                final int timeoutMills) throws RemotingException, InterruptedException {


        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, null);

        RegisterBrokerInfo brokerInfo = new RegisterBrokerInfo();
        brokerInfo.setBrokerId(brokerId);
        brokerInfo.setServerAddr(brokerAddr);
        brokerInfo.setBrokerName(brokerName);

        request.setBody(brokerInfo.encode());

        if (oneway) {
            try {
                this.nettyClient.invokeOneway(namesrvAddr, request, timeoutMills);

            } catch (RemotingException e) {

                logger.error("netty invokerOneWay error {}", e);

            }
            return null;
        }

        RegisterBrokerResult registerBrokerResult = null;
        RemotingCommand response = this.nettyClient.invokeSync(namesrvAddr, request, timeoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {


                if (response.getBody() != null) {

                    JsonSerializable.decode(request.getBody(), RegisterBrokerResult.class);
                }

            }
            default:
                break;
        }


        return registerBrokerResult;
    }

}
