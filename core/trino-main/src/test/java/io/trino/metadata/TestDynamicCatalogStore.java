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
import com.google.inject.Key;
import io.airlift.json.JsonCodec;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;
import io.trino.server.testing.TestingTrinoServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.util.Objects.requireNonNull;

@Test(singleThreaded = true)
public class TestDynamicCatalogStore
{
    private static final CatalogName CONNECTOR_ID = new CatalogName("connector_id");
    private static final Map<String, String> DYNAMIC_CATALOG_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("experimental.dynamic-catalog.enable", "true")
            .put("catalog.config-dir", requireNonNull(TestCatalogResource.class.getResource(".")).getFile())
            .buildOrThrow();

    private InMemoryNodeManager nodeManager;
    private DynamicCatalogStore dynamicCatalogStore;
    private TestingTrinoServer server;
    private TestingTrinoServer worker;

    @BeforeMethod
    public void setUp() throws NoSuchFieldException, IllegalAccessException
    {
        this.nodeManager = new InMemoryNodeManager();
        this.server = TestingTrinoServer.builder().setProperties(DYNAMIC_CATALOG_PROPERTIES).build();
        this.worker = TestingTrinoServer.builder().setCoordinator(false).setProperties(DYNAMIC_CATALOG_PROPERTIES).build();
        this.dynamicCatalogStore = server.getInstance(Key.get(DynamicCatalogStore.class));
        setUpWorkerNode(worker);
    }

    private void setUpWorkerNode(TestingTrinoServer worker) throws NoSuchFieldException, IllegalAccessException
    {
        com.google.common.collect.ImmutableList.Builder<InternalNode> nodeBuilder = com.google.common.collect.ImmutableList.builder();
        nodeBuilder.add(new InternalNode("worker", worker.getBaseUrl(), NodeVersion.UNKNOWN, false));
        com.google.common.collect.ImmutableList<InternalNode> nodes = nodeBuilder.build();
        nodeManager.addNode(CONNECTOR_ID, nodes);

        Field nodeManagerField = DynamicCatalogStore.class.getDeclaredField("nodeManager");
        nodeManagerField.setAccessible(true);
        nodeManagerField.set(dynamicCatalogStore, nodeManager);
    }

    @Test
    public void dispatchRegisterCatalogRequest()
    {
        dynamicCatalogStore.dispatchRequest(node -> {
            URI uri = node.getInternalUri();
            return preparePost()
                    .setUri(uri.resolve("/v1/catalog/test"))
                    .setHeader("Content-Type", "application/json; charset=utf8")
                    .setBodyGenerator(jsonBodyGenerator(JsonCodec.mapJsonCodec(String.class, String.class), Map.of(
                            "connector.name", "system",
                            "connection-url", "jdbc:postgresql://localhost:5432/postgres",
                            "connection-user", "postgres",
                            "connection-password", "postgres")))
                    .build();
        });

        DynamicCatalogStore workerCatalogStore = worker.getInstance(Key.get(DynamicCatalogStore.class));
        List<String> workerCatalogList = workerCatalogStore.catalogList();
        assertEquals(workerCatalogList.size(), 1);
        assertEquals(workerCatalogList.get(0), "test");
    }

    @Test(priority = 2)
    public void dispatchDeleteCatalogRequest()
    {
        dynamicCatalogStore.dispatchRequest(node -> {
            URI uri = node.getInternalUri();
            return prepareDelete().setUri(uri.resolve("/v1/catalog/test")).build();
        });

        DynamicCatalogStore workerCatalogStore = worker.getInstance(Key.get(DynamicCatalogStore.class));
        List<String> workerCatalogList = workerCatalogStore.catalogList();
        assertEquals(workerCatalogList.size(), 0);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception
    {
        closeAll(server);
    }
}
