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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.discovery.client.DiscoveryException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.net.ConnectException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.*;

public class EnvoyAnnouncer {
    private static final Logger log = Logger.get(EnvoyAnnouncer.class);

    private final EnvoyAnnouncementClient announcementClient;
    private final ScheduledExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Inject
    public EnvoyAnnouncer(EnvoyAnnouncementClient client) {

        requireNonNull(client, "client is null");

        this.announcementClient = client;
        this.executor = new ScheduledThreadPoolExecutor(2, daemonThreadsNamed("Envoy-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor() {
        return executorMBean;
    }

    public void start() {
        checkState(!executor.isShutdown(), "Envoy announcer has been destroyed");
        if (started.compareAndSet(false, true)) {
            // announce immediately, if discovery is running
            ListenableFuture<Duration> announce = announce(System.nanoTime(), new Duration(0, SECONDS));
            try {
                announce.get(30, SECONDS);
            } catch (Exception ignored) {
            }
        }
    }

    private ListenableFuture<Duration> announce(long delayStart, Duration expectedDelay) {
        // log announcement did not happen within 5 seconds of expected delay
        if (System.nanoTime() - (delayStart + expectedDelay.roundTo(NANOSECONDS)) > SECONDS.toNanos(5)) {
            log.error("Expected envoy announcement after %s, but announcement was delayed %s", expectedDelay, Duration.nanosSince(delayStart));
        }

        ListenableFuture<Duration> future = announcementClient.announce();

        Futures.addCallback(future, new FutureCallback<>() {
            @Override
            public void onSuccess(Duration expectedDelay) {

                expectedDelay = new Duration(expectedDelay.toMillis(), MILLISECONDS);
                scheduleNextAnnouncement(expectedDelay);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error(t.getMessage());
                scheduleNextAnnouncement(new Duration(10, SECONDS));
            }
        }, executor);

        return future;
    }

    @PreDestroy
    public void destroy() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(30, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // unannounce
        try {
            getFutureValue(announcementClient.unannounce(), DiscoveryException.class);
        } catch (DiscoveryException e) {
            if (e.getCause() instanceof ConnectException) {
                log.error("Cannot connect to envoy server for unannounce: %s", e.getCause().getMessage());
            } else {
                log.error(e);
            }
        }
    }

    private void scheduleNextAnnouncement(Duration expectedDelay) {
        // already stopped?  avoids rejection exception
        if (executor.isShutdown()) {
            return;
        }

        long delayStart = System.nanoTime();
        executor.schedule(() -> announce(delayStart, expectedDelay), expectedDelay.toMillis(), MILLISECONDS);
    }
}
