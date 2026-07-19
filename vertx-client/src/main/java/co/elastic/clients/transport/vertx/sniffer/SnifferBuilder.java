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

import co.elastic.clients.transport.vertx.VertxElasticsearchClient;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Sniffer builder. Helps creating a new {@link Sniffer}.
 */
public final class SnifferBuilder {
    public static final long DEFAULT_SNIFF_INTERVAL = TimeUnit.MINUTES.toMillis(5);
    public static final long DEFAULT_SNIFF_AFTER_FAILURE_DELAY = TimeUnit.MINUTES.toMillis(1);

    private final VertxElasticsearchClient client;
    private long sniffIntervalMillis = DEFAULT_SNIFF_INTERVAL;
    private long sniffAfterFailureDelayMillis = DEFAULT_SNIFF_AFTER_FAILURE_DELAY;
    private NodesSniffer nodesSniffer;

    SnifferBuilder(VertxElasticsearchClient client) {
        Objects.requireNonNull(client, "client cannot be null");
        this.client = client;
    }

    /**
     * Sets the interval between consecutive ordinary sniff executions in milliseconds.
     */
    public SnifferBuilder setSniffIntervalMillis(int sniffIntervalMillis) {
        if (sniffIntervalMillis <= 0) {
            throw new IllegalArgumentException("sniffIntervalMillis must be greater than 0");
        }
        this.sniffIntervalMillis = sniffIntervalMillis;
        return this;
    }

    /**
     * Sets the delay of a sniff execution scheduled after a failure (in milliseconds).
     */
    public SnifferBuilder setSniffAfterFailureDelayMillis(int sniffAfterFailureDelayMillis) {
        if (sniffAfterFailureDelayMillis <= 0) {
            throw new IllegalArgumentException("sniffAfterFailureDelayMillis must be greater than 0");
        }
        this.sniffAfterFailureDelayMillis = sniffAfterFailureDelayMillis;
        return this;
    }

    /**
     * Sets the {@link NodesSniffer} to be used to read hosts.
     */
    public SnifferBuilder setNodesSniffer(NodesSniffer nodesSniffer) {
        Objects.requireNonNull(nodesSniffer, "nodesSniffer cannot be null");
        this.nodesSniffer = nodesSniffer;
        return this;
    }

    /**
     * Creates the {@link Sniffer} based on the provided configuration.
     */
    public Sniffer build() {
        if (nodesSniffer == null) {
            this.nodesSniffer = new ElasticsearchNodesSniffer(client);
        }
        return new Sniffer(client, nodesSniffer, sniffIntervalMillis, sniffAfterFailureDelayMillis);
    }
}
