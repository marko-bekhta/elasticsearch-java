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

import io.vertx.core.buffer.Buffer;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * An HTTP request to be sent to an Elasticsearch node.
 */
public class Request {

    private final String method;
    private final String endpoint;
    private final Map<String, String> parameters;
    private final Map<String, String> headers;
    private final Buffer body;

    public Request(String method, String endpoint, Map<String, String> parameters,
                   Map<String, String> headers, Buffer body) {
        this.method = Objects.requireNonNull(method, "method cannot be null");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
        this.parameters = parameters != null ? parameters : Collections.emptyMap();
        this.headers = headers != null ? headers : Collections.emptyMap();
        this.body = body;
    }

    public Request(String method, String endpoint) {
        this(method, endpoint, Collections.emptyMap(), Collections.emptyMap(), null);
    }

    public String getMethod() {
        return method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Buffer getBody() {
        return body;
    }

    @Override
    public String toString() {
        return "Request{method=" + method + ", endpoint=" + endpoint + '}';
    }
}
