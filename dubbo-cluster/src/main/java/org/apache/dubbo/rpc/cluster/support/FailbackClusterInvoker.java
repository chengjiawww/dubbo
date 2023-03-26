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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FAILBACK_TIMES;
import static org.apache.dubbo.rpc.cluster.Constants.RETRIES_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FAILBACK_TASKS;
import static org.apache.dubbo.rpc.cluster.Constants.FAIL_BACK_TASKS_KEY;

/**
 * 失败自动恢复：
 * 当服务消费端调用服务出现异常后，在后台记录失败的请求，
 * 并按照一定的策略后期再进行充实，这种模式通常用于消息通知操作
 *
 * When fails, record failure requests and schedule for retry on a regular interval.
 * Especially useful for services of notification.
 *
 * <a href="http://en.wikipedia.org/wiki/Failback">Failback</a>
 */
public class FailbackClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailbackClusterInvoker.class);

    private static final long RETRY_FAILED_PERIOD = 5;

    private final int retries;

    private final int failbackTasks;

    private volatile Timer failTimer;

    public FailbackClusterInvoker(Directory<T> directory) {
        super(directory);

        int retriesConfig = getUrl().getParameter(RETRIES_KEY, DEFAULT_FAILBACK_TIMES);
        if (retriesConfig <= 0) {
            retriesConfig = DEFAULT_FAILBACK_TIMES;
        }
        int failbackTasksConfig = getUrl().getParameter(FAIL_BACK_TASKS_KEY, DEFAULT_FAILBACK_TASKS);
        if (failbackTasksConfig <= 0) {
            failbackTasksConfig = DEFAULT_FAILBACK_TASKS;
        }
        retries = retriesConfig;
        failbackTasks = failbackTasksConfig;
    }

    private void addFailed(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, Invoker<T> lastInvoker) {
        if (failTimer == null) {
            synchronized (this) {
                if (failTimer == null) {
                    // 创建定时任务，
                    failTimer = new HashedWheelTimer(
                            new NamedThreadFactory("failback-cluster-timer", true),
                            1,
                            TimeUnit.SECONDS, 32, failbackTasks);
                }
            }
        }
        // 启动定时任务 每隔5秒执行一次
        RetryTimerTask retryTimerTask = new RetryTimerTask(loadbalance, invocation, invokers, lastInvoker, retries, RETRY_FAILED_PERIOD);
        try {
            failTimer.newTimeout(retryTimerTask, RETRY_FAILED_PERIOD, TimeUnit.SECONDS);
        } catch (Throwable e) {
            logger.error("Failback background works error,invocation->" + invocation + ", exception: " + e.getMessage());
        }
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        Invoker<T> invoker = null;
        try {
            checkInvokers(invokers, invocation);
            // 选择 invoker
            invoker = select(loadbalance, invocation, invokers, null);
            // 进行调用
            return invoker.invoke(invocation);
        } catch (Throwable e) {
            // 如果调用过程中发生异常，此时仅打印错误日志，不抛出异常
            logger.error("Failback to invoke method " + invocation.getMethodName() + ", wait for retry in background. Ignored exception: "
                    + e.getMessage() + ", ", e);
            // 记录调用信息
            addFailed(loadbalance, invocation, invokers, invoker);
            // 返回一个空结果给服务消费者
            return AsyncRpcResult.newDefaultAsyncResult(null, null, invocation); // ignore
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        if (failTimer != null) {
            failTimer.stop();
        }
    }

    /**
     * RetryTimerTask
     */
    private class RetryTimerTask implements TimerTask {
        private final Invocation invocation;
        private final LoadBalance loadbalance;
        private final List<Invoker<T>> invokers;
        private final int retries;
        private final long tick;
        private Invoker<T> lastInvoker;
        private int retryTimes = 0;

        RetryTimerTask(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, Invoker<T> lastInvoker, int retries, long tick) {
            this.loadbalance = loadbalance;
            this.invocation = invocation;
            this.invokers = invokers;
            this.retries = retries;
            this.tick = tick;
            this.lastInvoker=lastInvoker;
        }

        @Override
        public void run(Timeout timeout) {
            try {
                // 选择一个 invoker
                Invoker<T> retryInvoker = select(loadbalance, invocation, invokers, Collections.singletonList(lastInvoker));
                lastInvoker = retryInvoker;
                retryInvoker.invoke(invocation);
            } catch (Throwable e) {
                logger.error("Failed retry to invoke method " + invocation.getMethodName() + ", waiting again.", e);
                // 失败次数达到重试次数则不再重试
                if ((++retryTimes) >= retries) {
                    logger.error("Failed retry times exceed threshold (" + retries + "), We have to abandon, invocation->" + invocation);
                } else {
                    // 否指再次重试
                    rePut(timeout);
                }
            }
        }

        // 任务放入定时器，再次重试
        private void rePut(Timeout timeout) {
            if (timeout == null) {
                return;
            }

            Timer timer = timeout.timer();
            if (timer.isStop() || timeout.isCancelled()) {
                return;
            }

            // 再次重试
            timer.newTimeout(timeout.task(), tick, TimeUnit.SECONDS);
        }
    }
}
