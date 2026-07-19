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
import co.elastic.clients.transport.vertx.Node.Roles;
import co.elastic.clients.transport.vertx.Request;
import co.elastic.clients.transport.vertx.Response;
import co.elastic.clients.transport.vertx.VertxElasticsearchClient;
import io.vertx.core.buffer.Buffer;
import jakarta.json.Json;
import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonParserFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Class responsible for sniffing the http hosts from elasticsearch through the nodes info api and returning them back.
 * Compatible with elasticsearch 2.x+.
 */
public final class ElasticsearchNodesSniffer implements NodesSniffer {

    private static final Log logger = LogFactory.getLog(ElasticsearchNodesSniffer.class);

    public static final long DEFAULT_SNIFF_REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

    private final VertxElasticsearchClient client;
    private final Request request;
    private final Scheme scheme;
    private final JsonParserFactory jsonFactory = Json.createParserFactory(Map.of());

    public ElasticsearchNodesSniffer(VertxElasticsearchClient client) {
        this(client, DEFAULT_SNIFF_REQUEST_TIMEOUT, Scheme.HTTP);
    }

    public ElasticsearchNodesSniffer(VertxElasticsearchClient client, long sniffRequestTimeoutMillis, Scheme scheme) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        if (sniffRequestTimeoutMillis < 0) {
            throw new IllegalArgumentException("sniffRequestTimeoutMillis must be greater than 0");
        }
        Map<String, String> params = new HashMap<>();
        params.put("timeout", sniffRequestTimeoutMillis + "ms");
        this.request = new Request("GET", "/_nodes/http", params, Collections.emptyMap(), null);
        this.scheme = Objects.requireNonNull(scheme, "scheme cannot be null");
    }

    @Override
    public List<Node> sniff() throws IOException {
        Response response = client.performRequest(request);
        return readHosts(response.getBody(), scheme, jsonFactory);
    }

    static List<Node> readHosts(Buffer body, Scheme scheme, JsonParserFactory jsonFactory) throws IOException {
        try (InputStream inputStream = new ByteArrayInputStream(body.getBytes())) {
            JsonParser parser = jsonFactory.createParser(inputStream);
            if (parser.next() != JsonParser.Event.START_OBJECT) {
                throw new IOException("expected data to start with an object");
            }
            List<Node> nodes = new ArrayList<>();
            while (parser.next() != JsonParser.Event.END_OBJECT) {
                if (parser.currentEvent() == JsonParser.Event.KEY_NAME && "nodes".equals(parser.getString())) {
                    parser.next();
                    assert parser.currentEvent() == JsonParser.Event.START_OBJECT;
                    while (parser.next() != JsonParser.Event.END_OBJECT) {
                        String nodeId = parser.getString();
                        Node node = readNode(nodeId, parser, scheme);
                        if (node != null) {
                            nodes.add(node);
                        }
                    }
                } else if (parser.currentEvent() == JsonParser.Event.START_OBJECT) {
                    parser.skipObject();
                }
            }
            return nodes;
        }
    }

    private static Node readNode(String nodeId, JsonParser parser, Scheme scheme) throws IOException {
        URI publishedHost = null;
        Set<URI> boundHosts = new HashSet<>();
        String name = null;
        String version = null;
        final Map<String, String> protoAttributes = new HashMap<>();

        boolean sawRoles = false;
        final Set<String> roles = new TreeSet<>();

        String fieldName = null;
        while (parser.next() != JsonParser.Event.END_OBJECT) {
            if (parser.currentEvent() == JsonParser.Event.KEY_NAME) {
                fieldName = parser.getString();
            } else if (parser.currentEvent() == JsonParser.Event.START_OBJECT) {
                if ("http".equals(fieldName)) {
                    while (parser.next() != JsonParser.Event.END_OBJECT) {
                        if (parser.currentEvent() == JsonParser.Event.KEY_NAME && "publish_address".equals(parser.getString())) {
                            parser.next();
                            String address = parser.getString();
                            String host;
                            URI publishAddressAsURI;

                            // ES7 cname/ip:port format
                            if (address.contains("/")) {
                                String[] cnameAndURI = address.split("/", 2);
                                publishAddressAsURI = URI.create(scheme + "://" + cnameAndURI[1]);
                                host = cnameAndURI[0];
                            } else {
                                publishAddressAsURI = URI.create(scheme + "://" + address);
                                host = publishAddressAsURI.getHost();
                            }
                            publishedHost = URI.create(
                                publishAddressAsURI.getScheme() + "://" + host + ":" + publishAddressAsURI.getPort()
                            );
                        } else if (parser.currentEvent() == JsonParser.Event.KEY_NAME && "bound_address".equals(parser.getString())) {
                            parser.next();
                            assert parser.currentEvent() == JsonParser.Event.START_ARRAY;
                            while (parser.next() != JsonParser.Event.END_ARRAY) {
                                URI boundAddressAsURI = URI.create(scheme + "://" + parser.getString());
                                boundHosts.add(URI.create(
                                    boundAddressAsURI.getScheme() + "://"
                                        + boundAddressAsURI.getHost() + ":" + boundAddressAsURI.getPort()
                                ));
                            }
                        } else if (parser.currentEvent() == JsonParser.Event.START_OBJECT) {
                            parser.skipObject();
                        }
                    }
                } else if ("attributes".equals(fieldName)) {
                    while (parser.next() != JsonParser.Event.END_OBJECT) {
                        if (parser.currentEvent() == JsonParser.Event.KEY_NAME) {
                            String key = parser.getString();
                            parser.next();
                            String value = parser.getString();
                            String oldValue = protoAttributes.put(key, value);
                            if (oldValue != null) {
                                throw new IOException("repeated attribute key [" + key + "]");
                            }
                        }
                    }
                } else if (fieldName != null) {
                    parser.skipObject();
                }
            } else if (parser.currentEvent() == JsonParser.Event.START_ARRAY) {
                if ("roles".equals(fieldName)) {
                    sawRoles = true;
                    while (parser.next() != JsonParser.Event.END_ARRAY) {
                        roles.add(parser.getString());
                    }
                } else {
                    parser.skipArray();
                }
            } else if (parser.currentEvent().name().equals(JsonParser.Event.VALUE_STRING.name())) {
                if ("version".equals(fieldName)) {
                    version = parser.getString();
                } else if ("name".equals(fieldName)) {
                    name = parser.getString();
                }
            }
        }

        if (publishedHost == null) {
            logger.debug("skipping node [" + nodeId + "] with http disabled");
            return null;
        }

        Map<String, List<String>> realAttributes = new HashMap<>(protoAttributes.size());
        List<String> keys = new ArrayList<>(protoAttributes.keySet());
        for (String key : keys) {
            if (key.endsWith(".0")) {
                String realKey = key.substring(0, key.length() - 2);
                List<String> values = new ArrayList<>();
                int i = 0;
                while (true) {
                    String value = protoAttributes.remove(realKey + "." + i);
                    if (value == null) {
                        break;
                    }
                    values.add(value);
                    i++;
                }
                realAttributes.put(realKey, unmodifiableList(values));
            }
        }
        for (Map.Entry<String, String> entry : protoAttributes.entrySet()) {
            realAttributes.put(entry.getKey(), singletonList(entry.getValue()));
        }

        if (version != null && version.startsWith("2.")) {
            boolean clientAttribute = v2RoleAttributeValue(realAttributes, "client", false);
            Boolean masterAttribute = v2RoleAttributeValue(realAttributes, "master", null);
            Boolean dataAttribute = v2RoleAttributeValue(realAttributes, "data", null);
            if ((masterAttribute == null && !clientAttribute) || (masterAttribute != null && masterAttribute)) {
                roles.add("master");
            }
            if ((dataAttribute == null && !clientAttribute) || (dataAttribute != null && dataAttribute)) {
                roles.add("data");
            }
        } else {
            assert sawRoles : "didn't see roles for [" + nodeId + "]";
        }
        assert boundHosts.contains(publishedHost) : "[" + nodeId + "] doesn't make sense! publishedHost should be in boundHosts";
        logger.trace("adding node [" + nodeId + "]");
        return new Node(publishedHost, boundHosts, name, version, new Roles(roles), unmodifiableMap(realAttributes));
    }

    private static Boolean v2RoleAttributeValue(Map<String, List<String>> attributes, String name, Boolean defaultValue)
        throws IOException {
        List<String> valueList = attributes.remove(name);
        if (valueList == null) {
            return defaultValue;
        }
        if (valueList.size() != 1) {
            throw new IOException("expected only a single attribute value for [" + name + "] but got " + valueList);
        }
        switch (valueList.get(0)) {
            case "true":
                return true;
            case "false":
                return false;
            default:
                throw new IOException("expected [" + name + "] to be either [true] or [false] but was [" + valueList.get(0) + "]");
        }
    }

    public enum Scheme {
        HTTP("http"),
        HTTPS("https");

        private final String name;

        Scheme(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
