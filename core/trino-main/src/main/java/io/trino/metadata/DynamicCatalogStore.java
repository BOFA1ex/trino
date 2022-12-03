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

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.airlift.concurrent.Threads;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.connector.ConnectorManager;

import javax.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final InternalNodeManager nodeManager;
    private final Announcer announcer;
    private final HttpClient httpClient;
    private final ScheduledExecutorService executor;
    private final ArrayBlockingQueue<Function<InternalNode, Request>> dynamicCatalogRequestQueue;
    private final File catalogConfigurationDir;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, InternalNodeManager nodeManager, Announcer announcer,
                               @ForNodeManager HttpClient httpClient,
                               DynamicCatalogConfig dynamicCatalogConfig, StaticCatalogStoreConfig staticCatalogStoreConfig)
    {
        this.connectorManager = connectorManager;
        this.nodeManager = nodeManager;
        this.announcer = announcer;
        this.httpClient = httpClient;
        this.catalogConfigurationDir = staticCatalogStoreConfig.getCatalogConfigurationDir();
        this.dynamicCatalogRequestQueue = new ArrayBlockingQueue<>(dynamicCatalogConfig.getRequestQueueCapacity(), true);
        this.executor = new ScheduledThreadPoolExecutor(2, Threads.daemonThreadsNamed("Dispatcher-Catalog-%s"));
        this.executor.schedule(() -> {
            if (dynamicCatalogRequestQueue.isEmpty()) {
                return;
            }
            try {
                dispatchRequest(dynamicCatalogRequestQueue.poll(5, TimeUnit.SECONDS));
            }
            catch (InterruptedException ignored) {
            }
        }, dynamicCatalogConfig.getRequestDispatchDelay(), dynamicCatalogConfig.getRequestDispatchTimeUnit());
    }

    @PreDestroy
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void destroy()
    {
        this.executor.shutdownNow();
        try {
            this.executor.awaitTermination(30L, TimeUnit.SECONDS);
        }
        catch (InterruptedException var2) {
            Thread.currentThread().interrupt();
        }
    }

    public List<String> catalogList()
    {
        final ServiceAnnouncement announcement = getTrinoAnnouncement();
        final String propertyValue = announcement.getProperties().get("connectorIds");

        if (propertyValue == null) {
            return Collections.emptyList();
        }

        return Splitter.on(",").trimResults().omitEmptyStrings().splitToList(propertyValue);
    }

    public void registerCatalog(String catalogName, Map<String, String> catalogProperties)
    {
        checkArgument(catalogName != null, "catalog name must not be null.");
        checkArgument(!catalogName.isBlank(), "catalog name must not be blank.");
        checkArgument(catalogProperties != null, "properties must not be null.");
        checkArgument(!catalogProperties.isEmpty(), "properties name must not be empty.");

        final String connectorName = catalogProperties.remove("connector.name");
        checkArgument(connectorName != null, "connector name must not be null.");
        checkArgument(!connectorName.isBlank(), "connector name must not be blank.");

        log.info("-- Loading catalog %s --", catalogName);
        connectorManager.dropConnection(catalogName);
        connectorManager.createCatalog(catalogName, connectorName, Map.copyOf(catalogProperties));
        updateConnectorIds(catalogName, List::add);

        dumpCatalogFile(catalogName, catalogProperties);
        dumpCatalogRequest(uri -> preparePost()
                .setUri(uri.resolve("/v1/catalog/" + catalogName))
                .setHeader("Content-Type", "application/json; charset=utf8")
                .setBodyGenerator(jsonBodyGenerator(JsonCodec.mapJsonCodec(String.class, String.class), catalogProperties))
                .build());
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    public void removeCatalog(String catalogName)
    {
        checkArgument(catalogName != null, "catalog name must not be null.");
        checkArgument(!catalogName.isBlank(), "catalog name must not be blank.");

        log.info("-- Removing catalog %s --", catalogName);
        connectorManager.dropConnection(catalogName);
        updateConnectorIds(catalogName, List::remove);

        rmCatalogFile(catalogName);

        dumpCatalogRequest(uri -> prepareDelete()
                .setUri(uri.resolve("/v1/catalog/" + catalogName))
                .setHeader("Content-Type", "application/json; charset=utf8")
                .build());
        log.info("-- Removed catalog %s --", catalogName);
    }

    void dispatchRequest(Function<InternalNode, Request> nodeRequestFunction)
    {
        final Set<InternalNode> activeNodes = nodeManager.getNodes(NodeState.ACTIVE);
        int dispatchCount = 0;
        for (InternalNode activeNode : activeNodes) {
            if (activeNode.isCoordinator()) {
                continue;
            }
            final Request request = nodeRequestFunction.apply(activeNode);
            StatusResponseHandler.StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
            if (response.getStatusCode() != 200) {
                log.warn("-- Dispatch catalog resource request %s --", activeNode);
                continue;
            }
            dispatchCount++;
        }
        if (dispatchCount == 0) {
            log.warn("Dispatch failed, no worker was received.");
            dynamicCatalogRequestQueue.offer(nodeRequestFunction);
        }
    }

    void dumpCatalogRequest(Function<URI, Request> requestFunc)
    {
        Function<InternalNode, URI> nodeURIFunction = InternalNode::getInternalUri;
        dynamicCatalogRequestQueue.offer(nodeURIFunction.andThen(requestFunc));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    void rmCatalogFile(String catalogName)
    {
        new File(catalogConfigurationDir, catalogName + ".properties").delete();
    }

    void dumpCatalogFile(String catalogName, Map<String, String> catalogProperties)
    {
        final Properties properties = new Properties();
        properties.putAll(catalogProperties);
        final Path propertiesPath = catalogConfigurationDir.toPath().resolve(catalogName + ".properties");
        try (OutputStream os = Files.newOutputStream(propertiesPath)) {
            properties.store(os, null);
        }
        catch (IOException e) {
            log.error("Dump catalog properties error.");
            throw new RuntimeException(e);
        }
    }

    void updateConnectorIds(String catalogName, BiConsumer<List<String>, String> accumulator)
    {
        final ServiceAnnouncement announcement = getTrinoAnnouncement();
        final Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        final String propertyValue = announcement.getProperties().get("connectorIds");

        final List<String> connectorIds = propertyValue == null ? new ArrayList<>(1) :
                Splitter.on(",").trimResults().omitEmptyStrings().splitToList(propertyValue);
        accumulator.accept(connectorIds, catalogName);
        properties.put("connectorIds", String.join(",", connectorIds));

        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();

        nodeManager.refreshNodes();
    }

    ServiceAnnouncement getTrinoAnnouncement()
    {
        final Set<ServiceAnnouncement> announcements = announcer.getServiceAnnouncements();
        for (ServiceAnnouncement serviceAnnouncement : announcer.getServiceAnnouncements()) {
            if ("trino".equals(serviceAnnouncement.getType())) {
                return serviceAnnouncement;
            }
        }
        throw new IllegalArgumentException("Trino announcement not found: " + announcements);
    }
}
