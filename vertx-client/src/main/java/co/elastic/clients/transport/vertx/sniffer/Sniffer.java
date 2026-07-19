/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package co.elastic.clients.transport.vertx.sniffer;

import co.elastic.clients.transport.vertx.Node;
import co.elastic.clients.transport.vertx.VertxElasticsearchClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class responsible for sniffing nodes from some source (default is elasticsearch itself) and setting them
 * to a provided instance of {@link VertxElasticsearchClient}. Must be created via {@link SnifferBuilder}.
 * A background task fetches the nodes through the {@link NodesSniffer} and sets them to the
 * {@link VertxElasticsearchClient} instance.
 */
public class Sniffer implements Closeable {

    private static final Log logger = LogFactory.getLog(Sniffer.class);
    private static final String SNIFFER_THREAD_NAME = "es_vertx_client_sniffer";

    private final NodesSniffer nodesSniffer;
    private final VertxElasticsearchClient client;
    private final long sniffIntervalMillis;
    private final long sniffAfterFailureDelayMillis;
    private final Scheduler scheduler;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private volatile ScheduledTask nextScheduledTask;

    Sniffer(VertxElasticsearchClient client, NodesSniffer nodesSniffer, long sniffInterval, long sniffAfterFailureDelay) {
        this(client, nodesSniffer, new DefaultScheduler(), sniffInterval, sniffAfterFailureDelay);
    }

    Sniffer(VertxElasticsearchClient client, NodesSniffer nodesSniffer, Scheduler scheduler,
            long sniffInterval, long sniffAfterFailureDelay) {
        this.nodesSniffer = nodesSniffer;
        this.client = client;
        this.sniffIntervalMillis = sniffInterval;
        this.sniffAfterFailureDelayMillis = sniffAfterFailureDelay;
        this.scheduler = scheduler;
        Task task = new Task(sniffIntervalMillis) {
            @Override
            public void run() {
                super.run();
                initialized.compareAndSet(false, true);
            }
        };
        scheduler.schedule(task, 0L);
    }

    /**
     * Schedule sniffing to run as soon as possible if it isn't already running.
     */
    public void sniffOnFailure() {
        if (initialized.get()) {
            if (this.nextScheduledTask.skip()) {
                scheduler.schedule(new Task(sniffAfterFailureDelayMillis), 0L);
            }
        }
    }

    enum TaskState {
        WAITING,
        SKIPPED,
        STARTED
    }

    class Task implements Runnable {
        final long nextTaskDelay;
        final AtomicReference<TaskState> taskState = new AtomicReference<>(TaskState.WAITING);

        Task(long nextTaskDelay) {
            this.nextTaskDelay = nextTaskDelay;
        }

        @Override
        public void run() {
            if (taskState.compareAndSet(TaskState.WAITING, TaskState.STARTED) == false) {
                return;
            }
            try {
                sniff();
            } catch (Exception e) {
                logger.error("error while sniffing nodes", e);
            } finally {
                Task task = new Task(sniffIntervalMillis);
                Future<?> future = scheduler.schedule(task, nextTaskDelay);
                ScheduledTask previousTask = nextScheduledTask;
                nextScheduledTask = new ScheduledTask(task, future);
                assert initialized.get() == false || previousTask.task.isSkipped() || previousTask.task.hasStarted()
                    : "task that we are replacing is neither cancelled nor has it ever started";
            }
        }

        boolean hasStarted() {
            return taskState.get() == TaskState.STARTED;
        }

        boolean skip() {
            return taskState.compareAndSet(TaskState.WAITING, TaskState.SKIPPED);
        }

        boolean isSkipped() {
            return taskState.get() == TaskState.SKIPPED;
        }
    }

    static final class ScheduledTask {
        final Task task;
        final Future<?> future;

        ScheduledTask(Task task, Future<?> future) {
            this.task = task;
            this.future = future;
        }

        boolean skip() {
            future.cancel(false);
            return task.skip();
        }
    }

    final void sniff() throws IOException {
        List<Node> sniffedNodes = nodesSniffer.sniff();
        if (logger.isDebugEnabled()) {
            logger.debug("sniffed nodes: " + sniffedNodes);
        }
        if (sniffedNodes.isEmpty()) {
            logger.warn("no nodes to set, nodes will be updated at the next sniffing round");
        } else {
            client.setNodes(sniffedNodes);
        }
    }

    @Override
    public void close() {
        if (initialized.get()) {
            nextScheduledTask.skip();
        }
        this.scheduler.shutdown();
    }

    /**
     * Returns a new {@link SnifferBuilder} to help with {@link Sniffer} creation.
     */
    public static SnifferBuilder builder(VertxElasticsearchClient client) {
        return new SnifferBuilder(client);
    }

    interface Scheduler {
        Future<?> schedule(Task task, long delayMillis);
        void shutdown();
    }

    static final class DefaultScheduler implements Scheduler {
        final ScheduledExecutorService executor;

        DefaultScheduler() {
            this(initScheduledExecutorService());
        }

        DefaultScheduler(ScheduledExecutorService executor) {
            this.executor = executor;
        }

        private static ScheduledExecutorService initScheduledExecutorService() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                1, new SnifferThreadFactory(SNIFFER_THREAD_NAME));
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        }

        @Override
        public Future<?> schedule(Task task, long delayMillis) {
            return executor.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void shutdown() {
            executor.shutdown();
            try {
                if (executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    return;
                }
                executor.shutdownNow();
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class SnifferThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        private final ThreadFactory originalThreadFactory;

        private SnifferThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
            this.originalThreadFactory = Executors.defaultThreadFactory();
        }

        @Override
        public Thread newThread(final Runnable r) {
            Thread t = originalThreadFactory.newThread(r);
            t.setName(namePrefix + "[T#" + threadNumber.getAndIncrement() + "]");
            t.setDaemon(true);
            return t;
        }
    }
}
