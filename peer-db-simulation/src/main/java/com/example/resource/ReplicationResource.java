// src/main/java/com/example/resource/ReplicationResource.java
package com.example.resource;

import com.example.service.ReplicationOrchestrator;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

@Path("/replication")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ReplicationResource {
    
    @Inject
    ReplicationOrchestrator orchestrator;
    
    @POST
    @Path("/start")
    public Response startReplication() {
        try {
            var estimate = orchestrator.estimatePerformance();
            var summary = orchestrator.replicateAllTables();
            
            var response = Map.of(
                "estimate", estimate,
                "summary", summary,
                "success", summary.results().stream().allMatch(r -> r.success())
            );
            
            return Response.ok(response).build();
            
        } catch (Exception e) {
            return Response.serverError()
                .entity(Map.of("error", e.getMessage()))
                .build();
        }
    }
    
    @GET
    @Path("/performance")
    public Response getPerformanceEstimate() {
        var estimate = orchestrator.estimatePerformance();
        return Response.ok(estimate).build();
    }
    
    @GET
    @Path("/health")
    public Response health() {
        return Response.ok(Map.of("status", "healthy")).build();
    }
}