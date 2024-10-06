/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

public class CoordintorAnnouncer {
    private final CoordinatorAnnouncerConfig coordinatorAnnouncerConfig;
    private final NodeInfo nodeInfo;
    private final HttpClient client;

    Logger log = Logger.get(CoordintorAnnouncer.class);

    @Inject
    public CoordintorAnnouncer(CoordinatorAnnouncerConfig coordinatorAnnouncerConfig, NodeInfo nodeInfo) {
        this.coordinatorAnnouncerConfig = requireNonNull(coordinatorAnnouncerConfig, "Announcer config is null");
        this.nodeInfo = requireNonNull(nodeInfo, "node info is null");
        this.client = HttpClient.newBuilder().build();
    }

    public void announce() {
        if (!coordinatorAnnouncerConfig.isEnabled()) {
            return;
        }

        URI targetURI = uriBuilderFrom(coordinatorAnnouncerConfig.getRegistryURI())
                .addParameter("name", nodeInfo.getNodeId())
                .addParameter("host", coordinatorAnnouncerConfig.getClusterHost())
                .addParameter("port", coordinatorAnnouncerConfig.getClusterPort())
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(targetURI)
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        CompletableFuture<HttpResponse<String>> futureResponse = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        try {
            log.info("Initiating cluster announcement to envoy against URL %s", targetURI);
            HttpResponse<String> response = futureResponse.get();
            log.info("Envoy announcement completed with status %d", response.statusCode());
        } catch (Exception e) {
            log.warn("Announcement to envoy failed with error %s", e.getCause());
        }
    }

    /**
     * Un-announce cluster against the envoy registry to avoid duplicates, if host ip changes without changes in nodeId
     */
    public void unannounce() {
        if (!coordinatorAnnouncerConfig.isEnabled()) {
            return;
        }

        URI targetURI = uriBuilderFrom(coordinatorAnnouncerConfig.getRegistryURI())
                .addParameter("name", nodeInfo.getNodeId())
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(targetURI)
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .DELETE()
                .build();

        CompletableFuture<HttpResponse<String>> futureResponse = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        try {
            log.info("Initiating cluster un-announcement to envoy");
            HttpResponse<String> response = futureResponse.get();
            log.info("Envoy un-announcement completed with status %d and message '%s'", response.statusCode(), response.body());
        } catch (Exception e) {
            log.warn("Un-announcement to envoy failed with error %s", e.getCause());
        }
    }
}
