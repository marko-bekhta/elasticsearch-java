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
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Client that connects to an Elasticsearch cluster through Vert.x HTTP.
 * <p>
 * Must be created using {@link VertxElasticsearchClientBuilder}. The hosts that are part of the cluster
 * need to be provided at creation time, but can also be replaced later by calling {@link #setNodes(Collection)}.
 * <p>
 * Requests are sent in a round-robin fashion. Failing hosts are marked dead and retried after a certain
 * amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously failed.
 */
public class VertxElasticsearchClient implements Closeable {

    private final Vertx vertx;
    private final HttpClient httpClient;
    private final RequestDispatcher dispatcher;
    private final boolean ownsVertx;

    VertxElasticsearchClient(Vertx vertx, HttpClient httpClient, RequestDispatcher dispatcher, boolean ownsVertx) {
        this.vertx = vertx;
        this.httpClient = httpClient;
        this.dispatcher = dispatcher;
        this.ownsVertx = ownsVertx;
    }

    /**
     * Returns a new {@link VertxElasticsearchClientBuilder} to help with client creation.
     */
    public static VertxElasticsearchClientBuilder builder(URI... hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts must not be null nor empty");
        }
        List<Node> nodes = new java.util.ArrayList<>();
        for (URI host : hosts) {
            nodes.add(new Node(host));
        }
        return new VertxElasticsearchClientBuilder(null, nodes);
    }

    /**
     * Returns a new {@link VertxElasticsearchClientBuilder} using the provided Vert.x instance.
     */
    public static VertxElasticsearchClientBuilder builder(Vertx vertx, URI... hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts must not be null nor empty");
        }
        List<Node> nodes = new java.util.ArrayList<>();
        for (URI host : hosts) {
            nodes.add(new Node(host));
        }
        return new VertxElasticsearchClientBuilder(vertx, nodes);
    }

    /**
     * Sends a request to the Elasticsearch cluster synchronously.
     * Blocks until the request is completed and returns its response or fails by throwing an exception.
     *
     * @param request the request to perform
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     */
    public Response performRequest(Request request) throws IOException {
        try {
            return performRequestAsync(request).toCompletionStage().toCompletableFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IOException(cause.getMessage(), cause);
        }
    }

    /**
     * Sends a request to the Elasticsearch cluster asynchronously.
     *
     * @param request the request to perform
     * @return a Future that will be completed with the response
     */
    public Future<Response> performRequestAsync(Request request) {
        return dispatcher.dispatch(httpClient, request);
    }

    /**
     * Replaces the nodes with which the client communicates.
     */
    public void setNodes(Collection<Node> nodes) {
        dispatcher.setNodes(nodes);
    }

    /**
     * Get the list of nodes that the client knows about.
     */
    public List<Node> getNodes() {
        return dispatcher.getNodes();
    }

    @Override
    public void close() throws IOException {
        try {
            httpClient.close().toCompletionStage().toCompletableFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
        }
        try {
            dispatcher.close();
        } catch (IOException e) {
            // ignore
        }
        if (ownsVertx) {
            try {
                vertx.close().toCompletionStage().toCompletableFuture().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
            }
        }
    }

    /**
     * Listener that allows to be notified whenever a failure happens. Useful when sniffing is enabled,
     * so that we can sniff on failure. The default implementation is a no-op.
     */
    public static class FailureListener {
        /**
         * Notifies that the node provided as argument has just failed.
         */
        public void onFailure(Node node) {
        }
    }
}
