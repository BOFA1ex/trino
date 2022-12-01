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
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.server.testing.TestingTrinoServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCatalogResource
{
    private HttpClient client;
    private TestingTrinoServer server;

    @BeforeMethod
    public void setup()
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("experimental.dynamic-catalog.enable", "true")
                        .put("catalog.config-dir", requireNonNull(TestCatalogResource.class.getResource(".")).getFile())
                        .buildOrThrow())
                .build();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception
    {
        closeAll(server, client);
    }

    @Test
    public void testRegisterCatalog()
    {
        String catalogName = "test";
        Request request = preparePost().setUri(server.getBaseUrl().resolve("/v1/catalog/" + catalogName))
                .setHeader("content-type", "application/json")
                .setBodyGenerator(jsonBodyGenerator(JsonCodec.mapJsonCodec(String.class, String.class), Map.of(
                        "connector.name", "system",
                        "connection-url", "jdbc:postgresql://localhost:5432/postgres",
                        "connection-user", "postgres",
                        "connection-password", "postgres")))
                .build();

        StatusResponseHandler.StatusResponse response = client.execute(request, createStatusResponseHandler());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test(priority = 2)
    public void testEditCatalog()
    {
        String catalogName = "test";
        Request request = preparePost().setUri(server.getBaseUrl().resolve("/v1/catalog/" + catalogName))
                .setHeader("content-type", "application/json")
                .setBodyGenerator(jsonBodyGenerator(JsonCodec.mapJsonCodec(String.class, String.class), Map.of(
                        "connector.name", "system",
                        "connection-url", "jdbc:postgresql://localhost:5432/postgres",
                        "connection-user", "postgres",
                        "connection-password", "modified")))
                .build();

        StatusResponseHandler.StatusResponse response = client.execute(request, createStatusResponseHandler());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test(priority = 3)
    public void testDeleteCatalog()
    {
        String catalogName = "test";
        Request request = prepareDelete().setUri(server.getBaseUrl().resolve("/v1/catalog/" + catalogName)).build();

        StatusResponseHandler.StatusResponse response = client.execute(request, createStatusResponseHandler());
        assertEquals(response.getStatusCode(), 200);
    }

    @Test
    public void catalogList()
    {
        Request request = prepareGet().setUri(server.getBaseUrl().resolve("/v1/catalog")).build();

        List<String> catalogList = client.execute(request, createJsonResponseHandler(JsonCodec.listJsonCodec(String.class)));
        assertTrue(catalogList.isEmpty());
    }
}
