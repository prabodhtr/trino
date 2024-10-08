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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigBinder;
import io.airlift.http.client.HttpClientBinder;

public class EnvoyAnnouncerModule extends AbstractConfigurationAwareModule {

    @Override
    protected void setup(Binder binder) {
        if (buildConfigObject(ServerConfig.class).isCoordinator() &&
                buildConfigObject(CoordinatorAnnouncerConfig.class).isEnabled()) {

            ConfigBinder.configBinder(binder).bindConfig(CoordinatorAnnouncerConfig.class);
            binder.bind(EnvoyAnnouncementClient.class).in(Scopes.SINGLETON);
            HttpClientBinder.httpClientBinder(binder).bindHttpClient("envoy", ForEnvoyAnnouncement.class);
            binder.bind(EnvoyAnnouncer.class).in(Scopes.SINGLETON);
        }

    }
}
