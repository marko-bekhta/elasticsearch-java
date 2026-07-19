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

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An HTTP response from an Elasticsearch node.
 */
public class Response {

    private final int statusCode;
    private final String statusMessage;
    private final MultiMap headers;
    private final Buffer body;
    private final URI node;

    public Response(int statusCode, String statusMessage, MultiMap headers, Buffer body, URI node) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = Objects.requireNonNull(headers, "headers cannot be null");
        this.body = body;
        this.node = Objects.requireNonNull(node, "node cannot be null");
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public MultiMap getHeaders() {
        return headers;
    }

    public Buffer getBody() {
        return body;
    }

    public URI getNode() {
        return node;
    }

    /**
     * Returns the value of the first header with the specified name, or {@code null} if not present.
     */
    public String getHeader(String name) {
        return headers.get(name);
    }

    /**
     * Optimized regular expression to test if a string matches the RFC 1123 date
     * format (with quotes and leading space).
     */
    private static final Pattern WARNING_HEADER_DATE_PATTERN = Pattern.compile("^ "
        + "\""
        + "(?>Mon|Tue|Wed|Thu|Fri|Sat|Sun), "
        + "\\d{2} "
        + "(?>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
        + "\\d{4} "
        + "\\d{2}:\\d{2}:\\d{2} "
        + "GMT"
        + "\"$");

    // tag::noformat
    private static final int WARNING_HEADER_DATE_LENGTH = 0
        + 1
        + 1
        + 3 + 1 + 1
        + 2 + 1
        + 3 + 1
        + 4 + 1
        + 2 + 1 + 2 + 1 + 2 + 1
        + 3
        + 1;
    // end::noformat

    private static boolean matchWarningHeaderPatternByPrefix(final String s) {
        return s.startsWith("299 Elasticsearch-") || s.startsWith("300 Elasticsearch-");
    }

    private static String extractWarningValueFromWarningHeader(final String s) {
        String warningHeader = s;

        if (s.length() > WARNING_HEADER_DATE_LENGTH) {
            final String possibleDateString = s.substring(s.length() - WARNING_HEADER_DATE_LENGTH);
            final Matcher matcher = WARNING_HEADER_DATE_PATTERN.matcher(possibleDateString);

            if (matcher.matches()) {
                warningHeader = warningHeader.substring(0, s.length() - WARNING_HEADER_DATE_LENGTH);
            }
        }

        final int firstQuote = warningHeader.indexOf('\"');
        final int lastQuote = warningHeader.length() - 1;
        final String warningValue = warningHeader.substring(firstQuote + 1, lastQuote);
        return warningValue;
    }

    /**
     * Returns a list of all warning headers returned in the response.
     */
    public List<String> getWarnings() {
        List<String> warnings = new ArrayList<>();
        List<String> warningHeaders = headers.getAll("Warning");
        if (warningHeaders != null) {
            for (String warning : warningHeaders) {
                if (matchWarningHeaderPatternByPrefix(warning)) {
                    warnings.add(extractWarningValueFromWarningHeader(warning));
                } else {
                    warnings.add(warning);
                }
            }
        }
        return warnings;
    }

    /**
     * Returns true if there is at least one warning header returned in the response.
     */
    public boolean hasWarnings() {
        List<String> warningHeaders = headers.getAll("Warning");
        return warningHeaders != null && !warningHeaders.isEmpty();
    }

    @Override
    public String toString() {
        return "Response{statusCode=" + statusCode + ", node=" + node + '}';
    }
}
