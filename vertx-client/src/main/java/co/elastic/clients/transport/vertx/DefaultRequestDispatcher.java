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

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;

/**
 * Default implementation of {@link RequestDispatcher} that provides round-robin node selection,
 * dead-node tracking with exponential backoff, and retry-across-nodes on failures.
 * <p>
 * Ported from Rest5Client's node management logic.
 */
public class DefaultRequestDispatcher implements RequestDispatcher {

    private static final Log logger = LogFactory.getLog(DefaultRequestDispatcher.class);

    private final AtomicInteger lastNodeIndex = new AtomicInteger(0);
    private final ConcurrentMap<URI, DeadHostState> blacklist = new ConcurrentHashMap<>();
    private final NodeSelector nodeSelector;
    private final VertxElasticsearchClient.FailureListener failureListener;
    private final String pathPrefix;
    private final boolean compressionEnabled;
    private final boolean metaHeaderEnabled;
    private final Map<String, String> defaultHeaders;
    private volatile List<Node> nodes;

    public DefaultRequestDispatcher(
        List<Node> nodes,
        NodeSelector nodeSelector,
        VertxElasticsearchClient.FailureListener failureListener,
        String pathPrefix,
        boolean compressionEnabled,
        boolean metaHeaderEnabled,
        Map<String, String> defaultHeaders
    ) {
        this.nodeSelector = Objects.requireNonNull(nodeSelector, "nodeSelector cannot be null");
        this.failureListener = failureListener != null ? failureListener : new VertxElasticsearchClient.FailureListener();
        this.pathPrefix = pathPrefix;
        this.compressionEnabled = compressionEnabled;
        this.metaHeaderEnabled = metaHeaderEnabled;
        this.defaultHeaders = defaultHeaders != null ? defaultHeaders : Collections.emptyMap();
        setNodes(nodes);
    }

    @Override
    public Future<Response> dispatch(HttpClient httpClient, Request request) {
        try {
            Iterator<Node> nodeIterator = nextNodes();
            return dispatchToNextNode(httpClient, request, nodeIterator, null);
        } catch (IOException e) {
            return Future.failedFuture(e);
        }
    }

    private Future<Response> dispatchToNextNode(
        HttpClient httpClient,
        Request request,
        Iterator<Node> nodeIterator,
        Exception previousException
    ) {
        if (!nodeIterator.hasNext()) {
            return Future.failedFuture(previousException != null
                ? previousException
                : new IOException("No nodes available"));
        }

        Node node = nodeIterator.next();
        URI nodeUri = node.getHost();

        String fullPath = buildUri(pathPrefix, request.getEndpoint(), request.getParameters());
        int port = nodeUri.getPort();
        if (port == -1) {
            port = "https".equals(nodeUri.getScheme()) ? 443 : 80;
        }

        return httpClient.request(new io.vertx.core.http.RequestOptions()
                .setMethod(HttpMethod.valueOf(request.getMethod().toUpperCase()))
                .setHost(nodeUri.getHost())
                .setPort(port)
                .setSsl("https".equals(nodeUri.getScheme()))
                .setURI(fullPath))
            .compose(httpClientRequest -> {
                applyHeaders(httpClientRequest, request);
                Buffer body = request.getBody();
                if (body != null) {
                    return httpClientRequest.send(body);
                } else {
                    return httpClientRequest.send();
                }
            })
            .compose(httpClientResponse ->
                httpClientResponse.body().map(responseBody -> {
                    MultiMap responseHeaders = httpClientResponse.headers();
                    return new Response(
                        httpClientResponse.statusCode(),
                        httpClientResponse.statusMessage(),
                        responseHeaders,
                        responseBody,
                        nodeUri
                    );
                })
            )
            .compose(response -> {
                int statusCode = response.getStatusCode();
                if (statusCode < 500) {
                    onResponse(node);
                    return Future.succeededFuture(response);
                }
                if (isRetryStatus(statusCode)) {
                    onFailure(node);
                    IOException retryException = new IOException(
                        "Received status " + statusCode + " from node " + nodeUri);
                    addSuppressedException(previousException, retryException);
                    if (nodeIterator.hasNext()) {
                        return dispatchToNextNode(httpClient, request, nodeIterator, retryException);
                    }
                    return Future.failedFuture(retryException);
                }
                // Non-retryable 5xx
                onResponse(node);
                return Future.succeededFuture(response);
            })
            .recover(throwable -> {
                if (throwable instanceof IOException && "No nodes available".equals(throwable.getMessage())) {
                    return Future.failedFuture(throwable);
                }
                onFailure(node);
                Exception cause = wrapException(throwable);
                addSuppressedException(previousException, cause);
                if (nodeIterator.hasNext()) {
                    return dispatchToNextNode(httpClient, request, nodeIterator, cause);
                }
                return Future.failedFuture(cause);
            });
    }

    private void applyHeaders(HttpClientRequest httpClientRequest, Request request) {
        // Apply default headers first
        for (Map.Entry<String, String> entry : defaultHeaders.entrySet()) {
            httpClientRequest.putHeader(entry.getKey(), entry.getValue());
        }
        // Apply request-specific headers (override defaults)
        for (Map.Entry<String, String> entry : request.getHeaders().entrySet()) {
            httpClientRequest.putHeader(entry.getKey(), entry.getValue());
        }
        if (compressionEnabled) {
            httpClientRequest.putHeader("Accept-Encoding", "gzip");
        }
    }

    private Iterator<Node> nextNodes() throws IOException {
        List<Node> currentNodes = this.nodes;
        return selectNodes(currentNodes, blacklist, lastNodeIndex, nodeSelector).iterator();
    }

    /**
     * Select nodes to try and sort them so that the first one will be tried initially, then the following
     * ones if the previous attempt failed. Package private for testing.
     */
    static Iterable<Node> selectNodes(
        List<Node> nodes,
        Map<URI, DeadHostState> blacklist,
        AtomicInteger lastNodeIndex,
        NodeSelector nodeSelector
    ) throws IOException {
        List<Node> livingNodes = new ArrayList<>(Math.max(0, nodes.size() - blacklist.size()));
        List<DeadNode> deadNodes = null;
        if (!blacklist.isEmpty()) {
            deadNodes = new ArrayList<>(blacklist.size());
            for (Node node : nodes) {
                DeadHostState deadness = blacklist.get(node.getHost());
                if (deadness == null || deadness.shallBeRetried()) {
                    livingNodes.add(node);
                } else {
                    deadNodes.add(new DeadNode(node, deadness));
                }
            }
        } else {
            livingNodes.addAll(nodes);
        }

        if (!livingNodes.isEmpty()) {
            List<Node> selectedLivingNodes = new ArrayList<>(livingNodes);
            nodeSelector.select(selectedLivingNodes);
            if (!selectedLivingNodes.isEmpty()) {
                Collections.rotate(selectedLivingNodes, lastNodeIndex.getAndIncrement());
                return selectedLivingNodes;
            }
        }

        if (deadNodes != null && !deadNodes.isEmpty()) {
            final List<DeadNode> selectedDeadNodes = new ArrayList<>(deadNodes);
            nodeSelector.select(() -> new DeadNodeIteratorAdapter(selectedDeadNodes.iterator()));
            if (!selectedDeadNodes.isEmpty()) {
                return singletonList(Collections.min(selectedDeadNodes).node);
            }
        }
        throw new IOException(
            "NodeSelector [" + nodeSelector + "] rejected all nodes, living: " + livingNodes + " and dead: " + deadNodes
        );
    }

    private void onResponse(Node node) {
        DeadHostState removedHost = this.blacklist.remove(node.getHost());
        if (logger.isDebugEnabled() && removedHost != null) {
            logger.debug("removed [" + node + "] from blacklist");
        }
    }

    private void onFailure(Node node) {
        DeadHostState previousDeadHostState = blacklist.putIfAbsent(
            node.getHost(),
            new DeadHostState(DeadHostState.DEFAULT_TIME_SUPPLIER)
        );
        if (previousDeadHostState == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("added [" + node + "] to blacklist");
            }
        } else {
            blacklist.replace(node.getHost(), previousDeadHostState,
                new DeadHostState(previousDeadHostState));
            if (logger.isDebugEnabled()) {
                logger.debug("updated [" + node + "] already in blacklist");
            }
        }
        failureListener.onFailure(node);
    }

    @Override
    public synchronized void setNodes(Collection<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("node list must not be null or empty");
        }
        Map<URI, Node> nodesByHost = new LinkedHashMap<>();
        for (Node node : nodes) {
            Objects.requireNonNull(node, "node cannot be null");
            nodesByHost.put(node.getHost(), node);
        }
        this.nodes = new ArrayList<>(nodesByHost.values());
        this.blacklist.clear();
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

    @Override
    public void close() {
        // No resources to release
    }

    static String buildUri(String pathPrefix, String path, Map<String, String> params) {
        Objects.requireNonNull(path, "path must not be null");
        String fullPath;
        if (pathPrefix != null && !pathPrefix.isEmpty()) {
            if (pathPrefix.endsWith("/") && path.startsWith("/")) {
                fullPath = pathPrefix.substring(0, pathPrefix.length() - 1) + path;
            } else if (pathPrefix.endsWith("/") || path.startsWith("/")) {
                fullPath = pathPrefix + path;
            } else {
                fullPath = pathPrefix + "/" + path;
            }
        } else {
            fullPath = path;
        }

        if (params != null && !params.isEmpty()) {
            StringBuilder sb = new StringBuilder(fullPath);
            sb.append('?');
            boolean first = true;
            for (Map.Entry<String, String> param : params.entrySet()) {
                if (!first) {
                    sb.append('&');
                }
                sb.append(URLEncoder.encode(param.getKey(), StandardCharsets.UTF_8));
                sb.append('=');
                sb.append(URLEncoder.encode(param.getValue(), StandardCharsets.UTF_8));
                first = false;
            }
            return sb.toString();
        }
        return fullPath;
    }

    private static boolean isRetryStatus(int statusCode) {
        return statusCode == 502 || statusCode == 503 || statusCode == 504;
    }

    private static void addSuppressedException(Exception suppressedException, Exception currentException) {
        if (suppressedException != null && suppressedException != currentException) {
            currentException.addSuppressed(suppressedException);
        }
    }

    private static Exception wrapException(Throwable throwable) {
        if (throwable instanceof Exception) {
            return (Exception) throwable;
        }
        return new RuntimeException(throwable.getMessage(), throwable);
    }

    /**
     * Contains a reference to a blacklisted node and the time until it is revived.
     */
    private static class DeadNode implements Comparable<DeadNode> {
        final Node node;
        final DeadHostState deadness;

        DeadNode(Node node, DeadHostState deadness) {
            this.node = node;
            this.deadness = deadness;
        }

        @Override
        public String toString() {
            return node.toString();
        }

        @Override
        public int compareTo(DeadNode rhs) {
            return deadness.compareTo(rhs.deadness);
        }
    }

    /**
     * Adapts an {@code Iterator<DeadNode>} into an {@code Iterator<Node>}.
     */
    private static class DeadNodeIteratorAdapter implements Iterator<Node> {
        private final Iterator<DeadNode> itr;

        private DeadNodeIteratorAdapter(Iterator<DeadNode> itr) {
            this.itr = itr;
        }

        @Override
        public boolean hasNext() {
            return itr.hasNext();
        }

        @Override
        public Node next() {
            return itr.next().node;
        }

        @Override
        public void remove() {
            itr.remove();
        }
    }
}
