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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDynamicCatalogStoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicCatalogConfig.class)
                .setRequestDispatchDelay(Duration.of(5000, ChronoUnit.MILLIS).toString())
                .setRequestQueueCapacity(200)
                .setEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("experimental.dynamic-catalog.enable", "true")
                .put("experimental.dynamic-catalog.request-queue-capacity", "400")
                .put("experimental.dynamic-catalog.request-dispatch-delay", "PT10S")
                .buildOrThrow();

        DynamicCatalogConfig expected = new DynamicCatalogConfig()
                .setRequestQueueCapacity(400)
                .setRequestDispatchDelay("PT10S")
                .setEnabled(true);

        assertFullMapping(properties, expected);
    }
}
