/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;


/**
 * 对心跳事件做了处理
 *
 * ExchangeReceiver
 */
public class HeaderExchangeHandler implements ChannelHandlerDelegate {

    protected static final Logger logger = LoggerFactory.getLogger(HeaderExchangeHandler.class);

    public static final String KEY_READ_TIMESTAMP = HeartbeatHandler.KEY_READ_TIMESTAMP;

    public static final String KEY_WRITE_TIMESTAMP = HeartbeatHandler.KEY_WRITE_TIMESTAMP;

    private final ExchangeHandler handler;

    public HeaderExchangeHandler(ExchangeHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.handler = handler;
    }

    /**
     * 处理响应消息
     * @param channel
     * @param response
     * @throws RemotingException
     */
    static void handleResponse(Channel channel, Response response) throws RemotingException {
        // 如果响应不为空，并且不是心跳事件的响应，则调用received
        if (response != null && !response.isHeartbeat()) {
            DefaultFuture.received(channel, response);
        }
    }

    private static boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(url.getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    void handlerEvent(Channel channel, Request req) throws RemotingException {
        if (req.getData() != null && req.getData().equals(Request.READONLY_EVENT)) {
            channel.setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        }
    }

    void handleRequest(final ExchangeChannel channel, Request req) throws RemotingException {
        // 创建一个Response实例
        Response res = new Response(req.getId(), req.getVersion());
        // 如果请求被破坏了
        if (req.isBroken()) {
            // 获得请求的数据包
            Object data = req.getData();

            String msg;
            // 如果数据为空
            if (data == null) {
                //消息设置为空
                msg = null;
                // 如果在这之前已经出现异常，也就是数据为Throwable类型
            } else if (data instanceof Throwable) {
                // 响应消息把异常信息返回
                msg = StringUtils.toString((Throwable) data);
            } else {
                // 返回请求数据
                msg = data.toString();
            }
            res.setErrorMessage("Fail to decode request due to: " + msg);
            // 设置错误请求的状态码
            res.setStatus(Response.BAD_REQUEST);

            // 发送该消息
            channel.send(res);
            return;
        }
        // find handler by message class.
        // 获得请求数据 也就是 RpcInvocation 对象
        Object msg = req.getData();
        try {
            // 继续向下调用 返回一个future
            // 这里我默认是使用dubbo协议，所以执行的是DubboProtocol的requestHandler的reply方法
            CompletionStage<Object> future = handler.reply(channel, msg);
            future.whenComplete((appResult, t) -> {
                try {
                    if (t == null) {
                        //设置调用结果状态为成功
                        res.setStatus(Response.OK);
                        // 把结果放入响应
                        res.setResult(appResult);
                    } else {
                        // 如果服务调用有异常，则设置结果状态码为服务错误
                        res.setStatus(Response.SERVICE_ERROR);
                        // 把报错信息放到响应中
                        res.setErrorMessage(StringUtils.toString(t));
                    }
                    // 发送该响应
                    channel.send(res);
                } catch (RemotingException e) {
                    logger.warn("Send result to consumer failed, channel is " + channel + ", msg is " + e);
                } finally {
                    // HeaderExchangeChannel.removeChannelIfDisconnected(channel);
                }
            });
        } catch (Throwable e) {
            // 如果在执行中抛出异常，则也算服务异常
            res.setStatus(Response.SERVICE_ERROR);
            res.setErrorMessage(StringUtils.toString(e));
            channel.send(res);
        }
    }

    @Override
    public void connected(Channel channel) throws RemotingException {
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.connected(exchangeChannel);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.disconnected(exchangeChannel);
        } finally {
            DefaultFuture.closeChannel(channel);
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        Throwable exception = null;
        try {
            channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
            ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
            try {
                handler.sent(exchangeChannel, message);
            } finally {
                HeaderExchangeChannel.removeChannelIfDisconnected(channel);
            }
        } catch (Throwable t) {
            exception = t;
        }
        if (message instanceof Request) {
            Request request = (Request) message;
            DefaultFuture.sent(channel, request);
        }
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else if (exception instanceof RemotingException) {
                throw (RemotingException) exception;
            } else {
                throw new RemotingException(channel.getLocalAddress(), channel.getRemoteAddress(),
                        exception.getMessage(), exception);
            }
        }
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        // 设置接收时间的时间戳属性值
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        // 获得通道
        final ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            // 如果消息是Request类型
            if (message instanceof Request) {
                // handle request.
                // 强制转化为Request
                Request request = (Request) message;
                // 如果该请求是事件心跳事件或者只读事件
                if (request.isEvent()) {
                    // 执行事件
                    handlerEvent(channel, request);
                } else {
                    // 如果是正常的调用请求，且需要响应
                    if (request.isTwoWay()) {
                        // 处理请求
                        handleRequest(exchangeChannel, request);
                    } else {
                        // 如果不需要响应，则继续下一步
                        handler.received(exchangeChannel, request.getData());
                    }
                }
            } else if (message instanceof Response) {
                // 处理响应
                handleResponse(channel, (Response) message);
            } else if (message instanceof String) {
                // 如果是telnet相关的请求
                if (isClientSide(channel)) {
                    // 如果是客户端侧，则直接抛出异常，因为客户端侧不支持telnet
                    Exception e = new Exception("Dubbo client can not supported string message: " + message + " in channel: " + channel + ", url: " + channel.getUrl());
                    logger.error(e.getMessage(), e);
                } else {
                    // 如果是服务端侧，则执行telnet命令
                    String echo = handler.telnet(channel, (String) message);
                    if (echo != null && echo.length() > 0) {
                        channel.send(echo);
                    }
                }
            } else {
                // 如果都不是，则继续下一步
                handler.received(exchangeChannel, message);
            }
        } finally {
            // 移除关闭或者不活跃的通道
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        if (exception instanceof ExecutionException) {
            ExecutionException e = (ExecutionException) exception;
            Object msg = e.getRequest();
            if (msg instanceof Request) {
                Request req = (Request) msg;
                if (req.isTwoWay() && !req.isHeartbeat()) {
                    Response res = new Response(req.getId(), req.getVersion());
                    res.setStatus(Response.SERVER_ERROR);
                    res.setErrorMessage(StringUtils.toString(e));
                    channel.send(res);
                    return;
                }
            }
        }
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.caught(exchangeChannel, exception);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }
}
