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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Metadata about a node running Elasticsearch.
 */
public class Node {

    private final URI host;
    private final Set<URI> boundHosts;
    private final String name;
    private final String version;
    private final Roles roles;
    private final Map<String, List<String>> attributes;

    /**
     * Create a {@linkplain Node} with metadata. All parameters except
     * {@code host} are nullable and implementations of {@link NodeSelector}
     * need to decide what to do in their absence.
     */
    public Node(URI host, Set<URI> boundHosts, String name, String version, Roles roles,
                Map<String, List<String>> attributes) {
        if (host == null) {
            throw new IllegalArgumentException("host cannot be null");
        }
        this.host = host;
        this.boundHosts = boundHosts;
        this.name = name;
        this.version = version;
        this.roles = roles;
        this.attributes = attributes;
    }

    /**
     * Create a {@linkplain Node} without any metadata.
     */
    public Node(URI host) {
        this(host, null, null, null, null, null);
    }

    /**
     * Contact information for the host.
     */
    public URI getHost() {
        return host;
    }

    /**
     * Addresses on which the host is listening. These are useful to have
     * around because they allow you to find a host based on any address it
     * is listening on.
     */
    public Set<URI> getBoundHosts() {
        return boundHosts;
    }

    /**
     * The {@code node.name} of the node.
     */
    public String getName() {
        return name;
    }

    /**
     * Version of Elasticsearch that the node is running or {@code null}
     * if we don't know the version.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Roles that the Elasticsearch process on the host has or {@code null}
     * if we don't know what roles the node has.
     */
    public Roles getRoles() {
        return roles;
    }

    /**
     * Attributes declared on the node.
     */
    public Map<String, List<String>> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[host=").append(host);
        if (boundHosts != null) {
            b.append(", bound=").append(boundHosts);
        }
        if (name != null) {
            b.append(", name=").append(name);
        }
        if (version != null) {
            b.append(", version=").append(version);
        }
        if (roles != null) {
            b.append(", roles=").append(roles);
        }
        if (attributes != null) {
            b.append(", attributes=").append(attributes);
        }
        return b.append(']').toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Node other = (Node) obj;
        return host.equals(other.host)
            && Objects.equals(boundHosts, other.boundHosts)
            && Objects.equals(name, other.name)
            && Objects.equals(version, other.version)
            && Objects.equals(roles, other.roles)
            && Objects.equals(attributes, other.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, boundHosts, name, version, roles, attributes);
    }

    /**
     * Role information about an Elasticsearch process.
     */
    public static final class Roles {

        private final Set<String> roles;

        public Roles(final Set<String> roles) {
            this.roles = new TreeSet<>(roles);
        }

        public boolean isMasterEligible() {
            return roles.contains("master");
        }

        @Deprecated
        public boolean isData() {
            return roles.contains("data");
        }

        public boolean hasDataRole() {
            return roles.contains("data");
        }

        public boolean hasDataContentRole() {
            return roles.contains("data_content");
        }

        public boolean hasDataHotRole() {
            return roles.contains("data_hot");
        }

        public boolean hasDataWarmRole() {
            return roles.contains("data_warm");
        }

        public boolean hasDataColdRole() {
            return roles.contains("data_cold");
        }

        public boolean hasDataFrozenRole() {
            return roles.contains("data_frozen");
        }

        public boolean canContainData() {
            return hasDataRole() || roles.stream().anyMatch(role -> role.startsWith("data_"));
        }

        public boolean isIngest() {
            return roles.contains("ingest");
        }

        @Override
        public String toString() {
            return String.join(",", roles);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Roles other = (Roles) obj;
            return roles.equals(other.roles);
        }

        @Override
        public int hashCode() {
            return roles.hashCode();
        }
    }
}
