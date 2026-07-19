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

import co.elastic.clients.transport.DefaultTransportOptions;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.http.TransportHttpClient;
import co.elastic.clients.util.BinaryData;
import io.vertx.core.buffer.Buffer;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Implements {@link TransportHttpClient} by wrapping a {@link VertxElasticsearchClient}.
 */
public class VertxTransportHttpClient implements TransportHttpClient {

    private final VertxElasticsearchClient vertxClient;

    public VertxTransportHttpClient(VertxElasticsearchClient vertxClient) {
        this.vertxClient = vertxClient;
    }

    @Override
    public TransportOptions createOptions(@Nullable TransportOptions options) {
        return options == null ? DefaultTransportOptions.EMPTY : options;
    }

    @Override
    public Response performRequest(
        String endpointId,
        @Nullable Node node,
        Request request,
        TransportOptions options
    ) throws IOException {
        co.elastic.clients.transport.vertx.Request vertxRequest = toVertxRequest(request, options);
        co.elastic.clients.transport.vertx.Response vertxResponse = vertxClient.performRequest(vertxRequest);
        return new VertxResponse(vertxResponse);
    }

    @Override
    public CompletableFuture<Response> performRequestAsync(
        String endpointId,
        @Nullable Node node,
        Request request,
        TransportOptions options
    ) {
        co.elastic.clients.transport.vertx.Request vertxRequest;
        try {
            vertxRequest = toVertxRequest(request, options);
        } catch (Exception e) {
            CompletableFuture<Response> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }

        return vertxClient.performRequestAsync(vertxRequest)
            .map(resp -> (Response) new VertxResponse(resp))
            .toCompletionStage()
            .toCompletableFuture();
    }

    @Override
    public void close() throws IOException {
        vertxClient.close();
    }

    private co.elastic.clients.transport.vertx.Request toVertxRequest(Request request, TransportOptions options) {
        Map<String, String> headers = new HashMap<>(request.headers());

        // Apply transport options headers (they take precedence)
        if (options != null) {
            for (Map.Entry<String, String> header : options.headers()) {
                headers.put(header.getKey(), header.getValue());
            }
        }

        Map<String, String> queryParams = new HashMap<>(request.queryParams());
        if (options != null) {
            queryParams.putAll(options.queryParameters());
        }

        Buffer body = null;
        Iterable<ByteBuffer> requestBody = request.body();
        if (requestBody != null) {
            body = toVertxBuffer(requestBody);
        }

        return new co.elastic.clients.transport.vertx.Request(
            request.method(),
            request.path(),
            queryParams,
            headers,
            body
        );
    }

    private static Buffer toVertxBuffer(Iterable<ByteBuffer> byteBuffers) {
        Buffer buffer = Buffer.buffer();
        for (ByteBuffer bb : byteBuffers) {
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            buffer.appendBytes(bytes);
        }
        return buffer;
    }

    /**
     * Wraps a Vert.x Response as a TransportHttpClient.Response.
     */
    private static class VertxResponse implements Response {
        private final co.elastic.clients.transport.vertx.Response response;

        VertxResponse(co.elastic.clients.transport.vertx.Response response) {
            this.response = response;
        }

        @Override
        public Node node() {
            return new Node(response.getNode());
        }

        @Override
        public int statusCode() {
            return response.getStatusCode();
        }

        @Nullable
        @Override
        public String header(String name) {
            return response.getHeader(name);
        }

        @Override
        public List<String> headers(String name) {
            List<String> values = response.getHeaders().getAll(name);
            return values != null ? values : Collections.emptyList();
        }

        @Nullable
        @Override
        public BinaryData body() throws IOException {
            Buffer body = response.getBody();
            if (body == null || body.length() == 0) {
                return null;
            }
            String contentType = response.getHeader("Content-Type");
            if (contentType == null) {
                contentType = "application/octet-stream";
            }
            return new InputStreamBinaryData(contentType, new ByteArrayInputStream(body.getBytes()));
        }

        @Nullable
        @Override
        public Object originalResponse() {
            return response;
        }

        @Override
        public void close() throws IOException {
            // Buffer is already fully materialized, nothing to release
        }
    }
}
