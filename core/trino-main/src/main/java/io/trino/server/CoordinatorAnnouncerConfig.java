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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.net.URI;

public class CoordinatorAnnouncerConfig
{
    private boolean enabled = false;

    private URI registryURI;

    private String clusterHost;

    private String clusterPort;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("cluster-announcement.enabled")
    public CoordinatorAnnouncerConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public URI getRegistryURI()
    {
        return registryURI;
    }

    @Config("cluster-announcement.registry-uri")
    @ConfigDescription("Registry base URI")
    public CoordinatorAnnouncerConfig setRegistryURI(URI uri)
    {
        this.registryURI = uri;
        return this;
    }

    public String getClusterPort() {
        return clusterPort;
    }

    @Config("cluster-announcement.port-to-register")
    public void setClusterPort(String clusterPort) {
        this.clusterPort = clusterPort;
    }

    public String getClusterHost() {
        return clusterHost;
    }

    @Config("cluster-announcement.host-to-register")
    public void setClusterHost(String clusterHost) {
        this.clusterHost = clusterHost;
    }
}
