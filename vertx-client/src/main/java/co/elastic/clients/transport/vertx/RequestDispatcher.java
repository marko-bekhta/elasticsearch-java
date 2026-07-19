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
import io.vertx.core.http.HttpClient;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

/**
 * Interface for dispatching HTTP requests with node management.
 */
public interface RequestDispatcher extends Closeable {

    /**
     * Dispatch a request through the given HttpClient, selecting the appropriate node.
     */
    Future<Response> dispatch(HttpClient httpClient, Request request);

    /**
     * Set the nodes the dispatcher should use.
     */
    void setNodes(Collection<Node> nodes);

    /**
     * Get the current list of nodes.
     */
    List<Node> getNodes();
}
