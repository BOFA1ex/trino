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
package io.trino.metadata;

import io.airlift.configuration.Config;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DynamicCatalogConfig
{
    private boolean enabled;
    private int requestQueueCapacity = 200;
    private long requestDispatchDelay = 5000;
    private final TimeUnit requestDispatchTimeUnit = TimeUnit.MILLISECONDS;

    @Config("experimental.dynamic-catalog.enable")
    public DynamicCatalogConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @Config("experimental.dynamic-catalog.request-queue-capacity")
    public DynamicCatalogConfig setRequestQueueCapacity(int requestQueueCapacity)
    {
        this.requestQueueCapacity = requestQueueCapacity;
        return this;
    }

    @Config("experimental.dynamic-catalog.request-dispatch-delay")
    public DynamicCatalogConfig setRequestDispatchDelay(String requestDispatchDuration)
    {
        final Duration duration = Duration.parse(requestDispatchDuration);
        this.requestDispatchDelay = duration.toMillis();
        return this;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public int getRequestQueueCapacity()
    {
        return requestQueueCapacity;
    }

    public long getRequestDispatchDelay()
    {
        return requestDispatchDelay;
    }

    public TimeUnit getRequestDispatchTimeUnit()
    {
        return requestDispatchTimeUnit;
    }
}
