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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.serialize.support.SerializableClassRegistry;
import org.apache.dubbo.common.serialize.support.SerializationOptimizer;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Transporter;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchangers;
import org.apache.dubbo.remoting.exchange.support.ExchangeHandlerAdapter;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.AbstractProtocol;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.LAZY_CONNECT_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.ON_CONNECT_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.ON_DISCONNECT_KEY;
import static org.apache.dubbo.remoting.Constants.DEFAULT_HEARTBEAT;
import static org.apache.dubbo.remoting.Constants.HEARTBEAT_KEY;
import static org.apache.dubbo.remoting.Constants.CHANNEL_READONLYEVENT_SENT_KEY;
import static org.apache.dubbo.remoting.Constants.CLIENT_KEY;
import static org.apache.dubbo.remoting.Constants.CODEC_KEY;
import static org.apache.dubbo.remoting.Constants.DEFAULT_REMOTING_CLIENT;
import static org.apache.dubbo.remoting.Constants.SERVER_KEY;
import static org.apache.dubbo.rpc.Constants.DEFAULT_REMOTING_SERVER;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.CALLBACK_SERVICE_KEY;
import static org.apache.dubbo.remoting.Constants.CONNECTIONS_KEY;
import static org.apache.dubbo.rpc.Constants.DEFAULT_STUB_EVENT;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.IS_CALLBACK_SERVICE;
import static org.apache.dubbo.rpc.Constants.IS_SERVER_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.OPTIMIZER_KEY;
import static org.apache.dubbo.rpc.Constants.STUB_EVENT_KEY;
import static org.apache.dubbo.rpc.Constants.STUB_EVENT_METHODS_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.SHARE_CONNECTIONS_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DEFAULT_SHARE_CONNECTIONS;


/**
 * dubbo protocol support.
 */
public class DubboProtocol extends AbstractProtocol {

    public static final String NAME = "dubbo";

    /**
     * 默认端口号
     */
    public static final int DEFAULT_PORT = 20880;
    /**
     * 回调名称
     */
    private static final String IS_CALLBACK_SERVICE_INVOKE = "_isCallBackServiceInvoke";
    /**
     * dubbo协议的单例
     */
    private static DubboProtocol INSTANCE;

    /**
     * 信息交换服务器集合 key：host:port  value：ExchangeServer
     */
    private final Map<String, ExchangeServer> serverMap = new ConcurrentHashMap<>();
    /**
     * <host:port,Exchanger> 信息交换客户端集合
     */
    private final Map<String, List<ReferenceCountExchangeClient>> referenceClientMap = new ConcurrentHashMap<>();
    /**
     * 锁集合
     */
    private final ConcurrentMap<String, Object> locks = new ConcurrentHashMap<>();
    /**
     * 序列化类名集合
     */
    private final Set<String> optimizers = new ConcurrentHashSet<>();
    /**
     * 本地存根服务方法集合
     */
    private final ConcurrentMap<String, String> stubServiceMethodsMap = new ConcurrentHashMap<>();

    /**
     * 新建一个请求处理器
     */
    private ExchangeHandler requestHandler = new ExchangeHandlerAdapter() {

        /**
         * 回复请求结果，返回的是请求结果
         * @param channel
         * @param message
         * @return
         * @throws RemotingException
         */
        @Override
        public CompletableFuture<Object> reply(ExchangeChannel channel, Object message) throws RemotingException {

            // 如果请求消息属于会话域
            if (!(message instanceof Invocation)) {
                throw new RemotingException(channel, "Unsupported request: "
                        + (message == null ? null : (message.getClass().getName() + ": " + message))
                        + ", channel: consumer: " + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress());
            }

            //强制类型转化
            Invocation inv = (Invocation) message;
            // 获得暴露的invoker
            Invoker<?> invoker = getInvoker(channel, inv);
            // need to consider backward-compatibility if it's a callback
            // 如果是回调服务
            if (Boolean.TRUE.toString().equals(inv.getAttachments().get(IS_CALLBACK_SERVICE_INVOKE))) {
                // 获得 方法定义
                String methodsStr = invoker.getUrl().getParameters().get("methods");
                boolean hasMethod = false;
                // 判断看是否有会话域中的方法
                if (methodsStr == null || !methodsStr.contains(",")) {
                    hasMethod = inv.getMethodName().equals(methodsStr);
                } else {
                    // 如果方法不止一个，则分割后遍历查询，找到了则设置为true
                    String[] methods = methodsStr.split(",");
                    for (String method : methods) {
                        if (inv.getMethodName().equals(method)) {
                            hasMethod = true;
                            break;
                        }
                    }
                }
                // 如果没有该方法，则打印告警日志
                if (!hasMethod) {
                    logger.warn(new IllegalStateException("The methodName " + inv.getMethodName()
                            + " not found in callback service interface ,invoke will be ignored."
                            + " please update the api interface. url is:"
                            + invoker.getUrl()) + " ,invocation is :" + inv);
                    return null;
                }
            }
            // 设置远程地址
            RpcContext.getContext().setRemoteAddress(channel.getRemoteAddress());
            // 调用下一个调用链，ProtocolFilterWrapper的CallbackRegistrationInvoker的invoke
            Result result = invoker.invoke(inv);
            return result.completionFuture().thenApply(Function.identity());
        }

        /**
         * 接收消息
         * @param channel
         * @param message
         * @throws RemotingException
         */
        @Override
        public void received(Channel channel, Object message) throws RemotingException {
            // 如果消息是会话域中的消息，则调用reply方法。
            if (message instanceof Invocation) {
                reply((ExchangeChannel) channel, message);

            } else {
                super.received(channel, message);
            }
        }

        @Override
        public void connected(Channel channel) throws RemotingException {
            // 接收连接事件
            invoke(channel, ON_CONNECT_KEY);
        }

        @Override
        public void disconnected(Channel channel) throws RemotingException {
            if (logger.isDebugEnabled()) {
                logger.debug("disconnected from " + channel.getRemoteAddress() + ",url:" + channel.getUrl());
            }
            // 接收断开连接事件
            invoke(channel, ON_DISCONNECT_KEY);
        }

        /**
         * 接收事件
         * @param channel
         * @param methodKey
         */
        private void invoke(Channel channel, String methodKey) {
            // 创建会话域
            Invocation invocation = createInvocation(channel, channel.getUrl(), methodKey);
            if (invocation != null) {
                try {
                    // 接收事件
                    received(channel, invocation);
                } catch (Throwable t) {
                    logger.warn("Failed to invoke event method " + invocation.getMethodName() + "(), cause: " + t.getMessage(), t);
                }
            }
        }

        /**
         * 创建会话域, 把url内的值加入到会话域的附加值中
         * @param channel
         * @param url
         * @param methodKey
         * @return
         */
        private Invocation createInvocation(Channel channel, URL url, String methodKey) {
            // 获得方法，methodKey是onconnect或者ondisconnect
            String method = url.getParameter(methodKey);
            if (method == null || method.length() == 0) {
                return null;
            }

            // 创建一个rpc会话域
            RpcInvocation invocation = new RpcInvocation(method, new Class<?>[0], new Object[0]);
            // 加入附加值path
            invocation.setAttachment(PATH_KEY, url.getPath());
            // 加入附加值group
            invocation.setAttachment(GROUP_KEY, url.getParameter(GROUP_KEY));
            // 加入附加值interface
            invocation.setAttachment(INTERFACE_KEY, url.getParameter(INTERFACE_KEY));
            // 加入附加值version
            invocation.setAttachment(VERSION_KEY, url.getParameter(VERSION_KEY));
            // 如果是本地存根服务，则加入附加值dubbo.stub.event为true
            if (url.getParameter(STUB_EVENT_KEY, false)) {
                invocation.setAttachment(STUB_EVENT_KEY, Boolean.TRUE.toString());
            }

            return invocation;
        }
    };

    public DubboProtocol() {
        INSTANCE = this;
    }

    public static DubboProtocol getDubboProtocol() {
        if (INSTANCE == null) {
            // load
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(DubboProtocol.NAME);
        }

        return INSTANCE;
    }

    public Collection<ExchangeServer> getServers() {
        return Collections.unmodifiableCollection(serverMap.values());
    }

    public Collection<Exporter<?>> getExporters() {
        return Collections.unmodifiableCollection(exporterMap.values());
    }

    Map<String, Exporter<?>> getExporterMap() {
        return exporterMap;
    }

    private boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(channel.getUrl().getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    Invoker<?> getInvoker(Channel channel, Invocation inv) throws RemotingException {
        boolean isCallBackServiceInvoke = false;
        boolean isStubServiceInvoke = false;
        int port = channel.getLocalAddress().getPort();
        String path = inv.getAttachments().get(PATH_KEY);

        // if it's callback service on client side
        isStubServiceInvoke = Boolean.TRUE.toString().equals(inv.getAttachments().get(STUB_EVENT_KEY));
        if (isStubServiceInvoke) {
            port = channel.getRemoteAddress().getPort();
        }

        //callback
        isCallBackServiceInvoke = isClientSide(channel) && !isStubServiceInvoke;
        if (isCallBackServiceInvoke) {
            path += "." + inv.getAttachments().get(CALLBACK_SERVICE_KEY);
            inv.getAttachments().put(IS_CALLBACK_SERVICE_INVOKE, Boolean.TRUE.toString());
        }

        String serviceKey = serviceKey(port, path, inv.getAttachments().get(VERSION_KEY), inv.getAttachments().get(GROUP_KEY));
        DubboExporter<?> exporter = (DubboExporter<?>) exporterMap.get(serviceKey);

        if (exporter == null) {
            throw new RemotingException(channel, "Not found exported service: " + serviceKey + " in " + exporterMap.keySet() + ", may be version or group mismatch " +
                    ", channel: consumer: " + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress() + ", message:" + inv);
        }

        return exporter.getInvoker();
    }

    public Collection<Invoker<?>> getInvokers() {
        return Collections.unmodifiableCollection(invokers);
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     * 远程暴露
     * @param invoker Service invoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        URL url = invoker.getUrl();

        // export service.
        // 得到服务key  group+"/"+serviceName+":"+serviceVersion+":"+port
        String key = serviceKey(url);
        // 创建exporter
        DubboExporter<T> exporter = new DubboExporter<T>(invoker, key, exporterMap);
        // 加入到集合
        exporterMap.put(key, exporter);

        //export an stub service for dispatching event
        Boolean isStubSupportEvent = url.getParameter(STUB_EVENT_KEY, DEFAULT_STUB_EVENT);
        Boolean isCallbackservice = url.getParameter(IS_CALLBACK_SERVICE, false);
        // 如果是本地存根事件而不是回调服务
        if (isStubSupportEvent && !isCallbackservice) {
            // 获得本地存根的方法
            String stubServiceMethods = url.getParameter(STUB_EVENT_METHODS_KEY);
            // 如果为空，则抛出异常
            if (stubServiceMethods == null || stubServiceMethods.length() == 0) {
                if (logger.isWarnEnabled()) {
                    logger.warn(new IllegalStateException("consumer [" + url.getParameter(INTERFACE_KEY) +
                            "], has set stubproxy support event ,but no stub methods founded."));
                }

            } else {
                // 加入集合
                stubServiceMethodsMap.put(url.getServiceKey(), stubServiceMethods);
            }
        }

        // 打开服务
        // 同一个机器的不同服务导出只会开启一个 NettyServer
        openServer(url);
        // 序列化
        optimizeSerialization(url);

        return exporter;
    }

    private void openServer(URL url) {
        // find server.
        // 服务端机器的地址 ip：port
        String key = url.getAddress();
        //client can export a service which's only for server to invoke
        // 只有服务端才会启动监听
        boolean isServer = url.getParameter(IS_SERVER_KEY, true);
        if (isServer) {
            // 获得信息交换服务器
            ExchangeServer server = serverMap.get(key);
            if (server == null) {
                synchronized (this) {
                    server = serverMap.get(key);
                    if (server == null) {
                        // 重新创建服务器对象，然后放入集合
                        serverMap.put(key, createServer(url));
                    }
                }
            } else {
                // server supports reset, use together with override
                server.reset(url);
            }
        }
    }

    private ExchangeServer createServer(URL url) {
        url = URLBuilder.from(url)
                // send readonly event when server closes, it's enabled by default
                .addParameterIfAbsent(CHANNEL_READONLYEVENT_SENT_KEY, Boolean.TRUE.toString())
                // enable heartbeat by default
                .addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT))
                .addParameter(CODEC_KEY, DubboCodec.NAME)
                .build();
        String str = url.getParameter(SERVER_KEY, DEFAULT_REMOTING_SERVER);

        if (str != null && str.length() > 0 && !ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(str)) {
            throw new RpcException("Unsupported server type: " + str + ", url: " + url);
        }

        ExchangeServer server;
        try {
            // 启动服务器
            server = Exchangers.bind(url, requestHandler);
        } catch (RemotingException e) {
            throw new RpcException("Fail to start server(url: " + url + ") " + e.getMessage(), e);
        }

        // 获得客户端侧设置的远程通信方式
        str = url.getParameter(CLIENT_KEY);
        if (str != null && str.length() > 0) {
            // 获得远程通信的实现集合
            Set<String> supportedTypes = ExtensionLoader.getExtensionLoader(Transporter.class).getSupportedExtensions();
            // 如果客户端侧设置的远程通信方式不在支持的方式中，则抛出异常
            if (!supportedTypes.contains(str)) {
                throw new RpcException("Unsupported client type: " + str);
            }
        }

        return server;
    }

    private void optimizeSerialization(URL url) throws RpcException {
        // 获得类名
        String className = url.getParameter(OPTIMIZER_KEY, "");
        if (StringUtils.isEmpty(className) || optimizers.contains(className)) {
            return;
        }

        logger.info("Optimizing the serialization process for Kryo, FST, etc...");

        try {
            // 加载类
            Class clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            if (!SerializationOptimizer.class.isAssignableFrom(clazz)) {
                throw new RpcException("The serialization optimizer " + className + " isn't an instance of " + SerializationOptimizer.class.getName());
            }

            // 强制类型转化为SerializationOptimizer
            SerializationOptimizer optimizer = (SerializationOptimizer) clazz.newInstance();

            if (optimizer.getSerializableClasses() == null) {
                return;
            }

            // 遍历序列化的类，把该类放入到集合进行缓存
            for (Class c : optimizer.getSerializableClasses()) {
                SerializableClassRegistry.registerClass(c);
            }

            // 加入到集合
            optimizers.add(className);

        } catch (ClassNotFoundException e) {
            throw new RpcException("Cannot find the serialization optimizer class: " + className, e);

        } catch (InstantiationException e) {
            throw new RpcException("Cannot instantiate the serialization optimizer class: " + className, e);

        } catch (IllegalAccessException e) {
            throw new RpcException("Cannot instantiate the serialization optimizer class: " + className, e);
        }
    }

    @Override
    public <T> Invoker<T> protocolBindingRefer(Class<T> serviceType, URL url) throws RpcException {
        optimizeSerialization(url);

        // create rpc invoker.
        DubboInvoker<T> invoker = new DubboInvoker<T>(serviceType, url, getClients(url), invokers);
        invokers.add(invoker);

        return invoker;
    }

    /**
     * 该方法是获得客户端集合的方法，分为共享客户端和非共享客户端。
     * 共享客户端是共用同一个连接，非共享客户端是每个客户端都有自己
     * 的一个连接。
     * @param url
     * @return
     */
    private ExchangeClient[] getClients(URL url) {
        // whether to share connection

        // 一个连接是否对于一个服务（不同服务是否共享）
        boolean useShareConnect = false;

        // 获得url中连接共享的配置 默认为0
        int connections = url.getParameter(CONNECTIONS_KEY, 0);
        List<ReferenceCountExchangeClient> shareClients = null;
        // if not configured, connection is shared, otherwise, one connection for one service
        // 如果为0，则是共享类，并且连接数为1
        // 如果没配置，则默认连接是共享的，否则每个服务单独有自己的连接
        if (connections == 0) {
            useShareConnect = true;

            /**
             * xml 配置优先级高于属性配置
             * The xml configuration should have a higher priority than properties.
             */
            String shareConnectionsStr = url.getParameter(SHARE_CONNECTIONS_KEY, (String) null);
            connections = Integer.parseInt(StringUtils.isBlank(shareConnectionsStr) ? ConfigUtils.getProperty(SHARE_CONNECTIONS_KEY,
                    DEFAULT_SHARE_CONNECTIONS) : shareConnectionsStr);
            // 获取共享 NettyClient
            shareClients = getSharedClient(url, connections);
        }

        // 初始化Client
        ExchangeClient[] clients = new ExchangeClient[connections];
        for (int i = 0; i < clients.length; i++) {
            // 如果共享，则返回已经存在的
            if (useShareConnect) {
                clients[i] = shareClients.get(i);

            } else { // 否则创建新的
                clients[i] = initClient(url);
            }
        }

        return clients;
    }

    /**
     * Get shared connection
     *
     * @param url
     * @param connectNum connectNum must be greater than or equal to 1
     */
    private List<ReferenceCountExchangeClient> getSharedClient(URL url, int connectNum) {
        String key = url.getAddress();
        // 从集合中取出客户端对象
        List<ReferenceCountExchangeClient> clients = referenceClientMap.get(key);

        // 如果不为空并且没关闭连接，则计数器加1，返回
        if (checkClientCanUse(clients)) {
            batchClientRefIncr(clients);
            return clients;
        }

        locks.putIfAbsent(key, new Object());
        synchronized (locks.get(key)) {
            clients = referenceClientMap.get(key);
            // dubbo check
            if (checkClientCanUse(clients)) {
                batchClientRefIncr(clients);
                return clients;
            }

            // connectNum must be greater than or equal to 1
            connectNum = Math.max(connectNum, 1);

            // If the clients is empty, then the first initialization is
            if (CollectionUtils.isEmpty(clients)) {
                // 否则新建一个连接
                clients = buildReferenceCountExchangeClientList(url, connectNum);
                referenceClientMap.put(key, clients);

            } else {
                for (int i = 0; i < clients.size(); i++) {
                    ReferenceCountExchangeClient referenceCountExchangeClient = clients.get(i);
                    // If there is a client in the list that is no longer available, create a new one to replace him.
                    if (referenceCountExchangeClient == null || referenceCountExchangeClient.isClosed()) {
                        clients.set(i, buildReferenceCountExchangeClient(url));
                        continue;
                    }

                    referenceCountExchangeClient.incrementAndGetCount();
                }
            }

            /**
             * I understand that the purpose of the remove operation here is to avoid the expired url key
             * always occupying this memory space.
             */
            locks.remove(key);

            return clients;
        }
    }

    /**
     * Check if the client list is all available
     *
     * @param referenceCountExchangeClients
     * @return true-available，false-unavailable
     */
    private boolean checkClientCanUse(List<ReferenceCountExchangeClient> referenceCountExchangeClients) {
        if (CollectionUtils.isEmpty(referenceCountExchangeClients)) {
            return false;
        }

        for (ReferenceCountExchangeClient referenceCountExchangeClient : referenceCountExchangeClients) {
            // As long as one client is not available, you need to replace the unavailable client with the available one.
            if (referenceCountExchangeClient == null || referenceCountExchangeClient.isClosed()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Add client references in bulk
     *
     * @param referenceCountExchangeClients
     */
    private void batchClientRefIncr(List<ReferenceCountExchangeClient> referenceCountExchangeClients) {
        if (CollectionUtils.isEmpty(referenceCountExchangeClients)) {
            return;
        }

        for (ReferenceCountExchangeClient referenceCountExchangeClient : referenceCountExchangeClients) {
            if (referenceCountExchangeClient != null) {
                referenceCountExchangeClient.incrementAndGetCount();
            }
        }
    }

    /**
     * Bulk build client
     *
     * @param url
     * @param connectNum
     * @return
     */
    private List<ReferenceCountExchangeClient> buildReferenceCountExchangeClientList(URL url, int connectNum) {
        List<ReferenceCountExchangeClient> clients = new ArrayList<>();

        for (int i = 0; i < connectNum; i++) {
            clients.add(buildReferenceCountExchangeClient(url));
        }

        return clients;
    }

    /**
     * Build a single client
     *
     * @param url
     * @return
     */
    private ReferenceCountExchangeClient buildReferenceCountExchangeClient(URL url) {
        ExchangeClient exchangeClient = initClient(url);

        return new ReferenceCountExchangeClient(exchangeClient);
    }

    /**
     * Create new connection
     *
     * @param url
     */
    private ExchangeClient initClient(URL url) {

        // client type setting.
        // 获得客户端的实现方法 默认netty4
        String str = url.getParameter(CLIENT_KEY, url.getParameter(SERVER_KEY, DEFAULT_REMOTING_CLIENT));

        // 添加编码器
        // 默认开启心跳
        url = url.addParameter(CODEC_KEY, DubboCodec.NAME);
        // enable heartbeat by default
        url = url.addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT));

        // BIO is not allowed since it has severe performance issue.
        if (str != null && str.length() > 0 && !ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(str)) {
            throw new RpcException("Unsupported client type: " + str + "," +
                    " supported client type is " + StringUtils.join(ExtensionLoader.getExtensionLoader(Transporter.class).getSupportedExtensions(), " "));
        }

        ExchangeClient client;
        try {
            // connection should be lazy
            // 是否需要延迟连接，，默认不开启
            if (url.getParameter(LAZY_CONNECT_KEY, false)) {
                // 创建延迟连接的客户端
                client = new LazyConnectExchangeClient(url, requestHandler);

            } else {
                // 否则就直接连接
                client = Exchangers.connect(url, requestHandler);
            }

        } catch (RemotingException e) {
            throw new RpcException("Fail to create remoting client for service(" + url + "): " + e.getMessage(), e);
        }

        return client;
    }

    @Override
    public void destroy() {
        for (String key : new ArrayList<>(serverMap.keySet())) {
            /**
             * Dubbo的ExchangeServer是Dubbo协议的服务端实现，它是Dubbo框架的核心组件之一。ExchangeServer主要作用是实现Dubbo协议的网络通信功能，包括服务暴露、请求接收、请求处理等一系列网络通信相关的功能。
             *
             * Dubbo的ExchangeServer是基于Netty实现的，通过Netty的高性能网络通信能力，实现了服务端和客户端之间的请求和响应的通信。ExchangeServer在Dubbo中扮演了重要的角色，为Dubbo框架提供了强大的网络通信能力和灵活的拓展性。
             *
             * 在Dubbo中，每个服务都会对应一个ExchangeServer实例，ExchangeServer会监听并绑定一个本地端口，等待客户端的请求。当客户端发起请求时，ExchangeServer会接收请求并将请求分发给对应的服务进行处理。同时，ExchangeServer也负责将服务的响应结果发送给客户端。通过ExchangeServer，Dubbo实现了高效、可靠的服务通信，保证了Dubbo框架的高可用性和高性能。
             */
            ExchangeServer server = serverMap.remove(key);

            if (server == null) {
                continue;
            }

            try {
                if (logger.isInfoEnabled()) {
                    logger.info("Close dubbo server: " + server.getLocalAddress());
                }

                server.close(ConfigurationUtils.getServerShutdownTimeout());

            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }

        for (String key : new ArrayList<>(referenceClientMap.keySet())) {
            List<ReferenceCountExchangeClient> clients = referenceClientMap.remove(key);

            if (CollectionUtils.isEmpty(clients)) {
                continue;
            }

            for (ReferenceCountExchangeClient client : clients) {
                closeReferenceCountExchangeClient(client);
            }
        }

        stubServiceMethodsMap.clear();
        super.destroy();
    }

    /**
     * close ReferenceCountExchangeClient
     *
     * @param client
     */
    private void closeReferenceCountExchangeClient(ReferenceCountExchangeClient client) {
        if (client == null) {
            return;
        }

        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close dubbo connect: " + client.getLocalAddress() + "-->" + client.getRemoteAddress());
            }

            client.close(ConfigurationUtils.getServerShutdownTimeout());

            // TODO
            /**
             * At this time, ReferenceCountExchangeClient#client has been replaced with LazyConnectExchangeClient.
             * Do you need to call client.close again to ensure that LazyConnectExchangeClient is also closed?
             */

        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }
}
