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

import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;

@Path("/v1/catalog")
public class CatalogResource
{
    private final DynamicCatalogStore dynamicCatalogStore;

    @Inject
    public CatalogResource(DynamicCatalogStore dynamicCatalogStore)
    {
        this.dynamicCatalogStore = dynamicCatalogStore;
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @ResourceSecurity(PUBLIC)
    public Response catalogList()
    {
        return Response.ok(dynamicCatalogStore.catalogList()).build();
    }

    @Path("/{catalogName}")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ResourceSecurity(PUBLIC)
    public Response registerCatalog(@PathParam("catalogName") String catalogName, Map<String, String> catalogProperties)
    {
        dynamicCatalogStore.registerCatalog(catalogName, catalogProperties);
        return Response.ok().build();
    }

    @Path("/{catalogName}")
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @ResourceSecurity(PUBLIC)
    public Response removeCatalog(@PathParam("catalogName") String catalogName)
    {
        dynamicCatalogStore.removeCatalog(catalogName);
        return Response.ok().build();
    }
}
