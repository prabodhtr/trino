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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.discovery.client.DiscoveryException;
import io.airlift.http.client.*;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class EnvoyAnnouncementClient {

    static Duration DEFAULT_DELAY = new Duration(5, TimeUnit.MINUTES);

    private static final MediaType MEDIA_TYPE_JSON = MediaType.create("application", "json");
    private final CoordinatorAnnouncerConfig config;
    private final NodeInfo nodeInfo;
    private final HttpClient httpClient;

    @Inject
    public EnvoyAnnouncementClient(
            CoordinatorAnnouncerConfig coordinatorAnnouncerConfig,
            NodeInfo nodeInfo,
            @ForEnvoyAnnouncement HttpClient httpClient) {
        this.config = requireNonNull(coordinatorAnnouncerConfig, "registryURI is null");
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }


    public ListenableFuture<Duration> announce() {
        URI uri = config.getRegistryURI();
        if (uri == null) {
            return immediateFailedFuture(new DiscoveryException("No discovery servers are available"));
        }

        Request request = preparePost()
                .setUri(createAnnouncementLocation(uri, nodeInfo.getNodeId(), config.getClusterHost(), config.getClusterPort()))
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .setHeader("Content-Type", MEDIA_TYPE_JSON.toString())
                .build();

        return httpClient.executeAsync(request, new EnvoyResponseHandler<Duration>("envoyAnnouncement", uri) {
            @Override
            public Duration handle(Request request, Response response) throws RuntimeException {
                int statusCode = response.getStatusCode();
                if (!isSuccess(statusCode)) {
                    throw new RuntimeException(String.format(
                            "Announcement to envoy failed with status code %s: %s",
                            statusCode,
                            getBodyForError(response)));
                }

                Duration maxAge = extractMaxAge(response);
                return maxAge;
            }
        });
    }

    public ListenableFuture<Void> unannounce() {
        URI uri = config.getRegistryURI();
        if (uri == null) {
            return immediateFuture(null);
        }

        Request request = prepareDelete()
                .setUri(
                        uriBuilderFrom(uri)
                                .addParameter("name", nodeInfo.getNodeId())
                                .build()
                )
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .build();

        return httpClient.executeAsync(request, new EnvoyResponseHandler<>("envoyUnannouncement", uri));
    }

    @VisibleForTesting
    static URI createAnnouncementLocation(URI baseUri, String nodeId, String host, String port) {
        return uriBuilderFrom(baseUri)
                .addParameter("name", nodeId)
                .addParameter("host", host)
                .addParameter("port", port)
                .build();
    }

    private static boolean isSuccess(int statusCode) {
        return statusCode / 100 == 2;
    }

    private static Duration extractMaxAge(Response response) {
        String header = response.getHeader(HttpHeaders.CACHE_CONTROL);
        if (header != null) {
            CacheControl cacheControl = CacheControl.valueOf(header);
            if (cacheControl.getMaxAge() > 0) {
                return new Duration(cacheControl.getMaxAge(), TimeUnit.SECONDS);
            }
        }
        return DEFAULT_DELAY;
    }

    private static String getBodyForError(Response response) {
        try {
            return CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));
        } catch (IOException e) {
            return "(error getting body)";
        }
    }

    private class EnvoyResponseHandler<T>
            implements ResponseHandler<T, DiscoveryException> {
        private final String name;
        private final URI uri;

        protected EnvoyResponseHandler(String name, URI uri) {
            this.name = name;
            this.uri = uri;
        }

        @Override
        public T handle(Request request, Response response) {
            return null;
        }

        @Override
        public final T handleException(Request request, Exception exception) {
            if (exception instanceof InterruptedException) {
                throw new DiscoveryException(name + " was interrupted for " + uri);
            }
            if (exception instanceof CancellationException) {
                throw new DiscoveryException(name + " was canceled for " + uri);
            }
            if (exception instanceof DiscoveryException) {
                throw (DiscoveryException) exception;
            }

            throw new DiscoveryException(name + " failed for " + uri, exception);
        }
    }
}
