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

package co.elastic.clients.transport.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.PoolOptions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for {@link VertxElasticsearchClient}.
 */
public class VertxElasticsearchClientBuilder {

    private Vertx vertx;
    private final List<Node> nodes;
    private HttpClientOptions httpClientOptions;
    private PoolOptions poolOptions;
    private Map<String, String> defaultHeaders = Collections.emptyMap();
    private String pathPrefix;
    private NodeSelector nodeSelector = NodeSelector.ANY;
    private VertxElasticsearchClient.FailureListener failureListener;
    private boolean compressionEnabled = false;
    private boolean metaHeaderEnabled = true;
    private RequestDispatcher dispatcher;

    VertxElasticsearchClientBuilder(Vertx vertx, List<Node> nodes) {
        Objects.requireNonNull(nodes, "nodes cannot be null");
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        this.vertx = vertx;
        this.nodes = nodes;
    }

    public VertxElasticsearchClientBuilder setVertx(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }

    public VertxElasticsearchClientBuilder setHttpClientOptions(HttpClientOptions httpClientOptions) {
        this.httpClientOptions = httpClientOptions;
        return this;
    }

    public VertxElasticsearchClientBuilder setPoolOptions(PoolOptions poolOptions) {
        this.poolOptions = poolOptions;
        return this;
    }

    public VertxElasticsearchClientBuilder setDefaultHeaders(Map<String, String> defaultHeaders) {
        this.defaultHeaders = Objects.requireNonNull(defaultHeaders, "defaultHeaders cannot be null");
        return this;
    }

    public VertxElasticsearchClientBuilder setPathPrefix(String pathPrefix) {
        this.pathPrefix = cleanPathPrefix(pathPrefix);
        return this;
    }

    public VertxElasticsearchClientBuilder setNodeSelector(NodeSelector nodeSelector) {
        this.nodeSelector = Objects.requireNonNull(nodeSelector, "nodeSelector cannot be null");
        return this;
    }

    public VertxElasticsearchClientBuilder setFailureListener(VertxElasticsearchClient.FailureListener failureListener) {
        this.failureListener = failureListener;
        return this;
    }

    public VertxElasticsearchClientBuilder setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
        return this;
    }

    public VertxElasticsearchClientBuilder setMetaHeaderEnabled(boolean metaHeaderEnabled) {
        this.metaHeaderEnabled = metaHeaderEnabled;
        return this;
    }

    public VertxElasticsearchClientBuilder setRequestDispatcher(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        return this;
    }

    /**
     * Build the {@link VertxElasticsearchClient}.
     */
    public VertxElasticsearchClient build() {
        boolean ownsVertx = false;
        Vertx vertxInstance = this.vertx;
        if (vertxInstance == null) {
            vertxInstance = Vertx.vertx();
            ownsVertx = true;
        }

        HttpClientOptions options = this.httpClientOptions != null
            ? this.httpClientOptions
            : new HttpClientOptions();

        if (compressionEnabled) {
            options.setDecompressionSupported(true);
        }

        HttpClient httpClient;
        if (poolOptions != null) {
            httpClient = vertxInstance.createHttpClient(options, poolOptions);
        } else {
            httpClient = vertxInstance.createHttpClient(options);
        }

        RequestDispatcher dispatcherInstance = this.dispatcher;
        if (dispatcherInstance == null) {
            dispatcherInstance = new DefaultRequestDispatcher(
                nodes,
                nodeSelector,
                failureListener,
                pathPrefix,
                compressionEnabled,
                metaHeaderEnabled,
                defaultHeaders
            );
        }

        return new VertxElasticsearchClient(vertxInstance, httpClient, dispatcherInstance, ownsVertx);
    }

    private static String cleanPathPrefix(String pathPrefix) {
        if (pathPrefix == null || pathPrefix.isEmpty()) {
            return pathPrefix;
        }
        if (!pathPrefix.startsWith("/")) {
            pathPrefix = "/" + pathPrefix;
        }
        if (pathPrefix.endsWith("/")) {
            pathPrefix = pathPrefix.substring(0, pathPrefix.length() - 1);
        }
        return pathPrefix;
    }
}
