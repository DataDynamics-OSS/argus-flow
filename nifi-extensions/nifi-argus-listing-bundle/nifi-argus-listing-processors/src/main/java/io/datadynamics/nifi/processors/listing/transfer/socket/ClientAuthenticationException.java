/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle, design/listing-processors-port.md):
 *   nifi-extension-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/socket/ClientAuthenticationException.java
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.processors.listing.transfer.socket;

/**
 * Client Authentication Exception for authentication failures during Client configuration
 */
public class ClientAuthenticationException extends ClientConfigurationException {

    public ClientAuthenticationException(final String message) {
        super(message);
    }

    public ClientAuthenticationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
