// src/main/java/com/example/resource/StatusResource.java
package com.example.resource;

import com.example.service.StatusService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

@Path("/status")
@Produces(MediaType.APPLICATION_JSON)
public class StatusResource {
    
    @Inject
    StatusService statusService;
    
    @GET
    @Path("/migration")
    public Response getMigrationStatus() {
        try {
            var status = statusService.getCurrentStatus();
            return Response.ok(status).build();
        } catch (Exception e) {
            return Response.serverError()
                .entity(Map.of("error", "Failed to get migration status: " + e.getMessage()))
                .build();
        }
    }
    
    @GET
    @Path("/migration/detailed")
    public Response getDetailedMigrationStatus() {
        try {
            var detailedStatus = statusService.getDetailedStatus();
            return Response.ok(detailedStatus).build();
        } catch (Exception e) {
            return Response.serverError()
                .entity(Map.of("error", "Failed to get detailed migration status: " + e.getMessage()))
                .build();
        }
    }
    
    @GET
    @Path("/migration/tables/{tableName}")
    public Response getTableStatus(@jakarta.ws.rs.PathParam("tableName") String tableName) {
        try {
            var tableStatus = statusService.getTableDetailedStatus(tableName);
            return Response.ok(tableStatus).build();
        } catch (Exception e) {
            return Response.serverError()
                .entity(Map.of("error", "Failed to get table status: " + e.getMessage()))
                .build();
        }
    }
    
    @GET
    @Path("/migration/summary")
    public Response getMigrationSummary() {
        try {
            System.out.println("DEBUG: Getting migration summary...");
            var status = statusService.getCurrentStatus();
            System.out.println("DEBUG: Got status: " + status);
            
            var summary = Map.of(
                "totalRecordsMigrated", status.totalRecordsMigrated(),
                "startTime", status.startTime(),
                "endTime", status.endTime(),
                "isRunning", status.endTime() == null,
                "tableCount", status.tableStatuses().size(),
                "throughput", Map.of(
                    "recordsPerSecond", status.throughput().recordsPerSecond(),
                    "recordsPerMinute", status.throughput().recordsPerMinute(),
                    "recordsPerHour", status.throughput().recordsPerHour()
                ),
                "overallProgress", status.overallProgress()
            );
            
            System.out.println("DEBUG: Summary created: " + summary);
            return Response.ok(summary).build();
        } catch (Exception e) {
            System.err.println("ERROR in getMigrationSummary: " + e.getMessage());
            e.printStackTrace();
            return Response.serverError()
                .entity(Map.of("error", "Failed to get migration summary: " + e.getMessage()))
                .build();
        }
    }
    @GET
    @Path("/health")
    public Response health() {
        try {
            // Simple health check that doesn't depend on complex state
            var health = Map.of(
                "status", "healthy",
                "service", "postgresql-replicator",
                "timestamp", System.currentTimeMillis()
            );
            
            return Response.ok(health).build();
        } catch (Exception e) {
            return Response.serverError()
                .entity(Map.of("status", "unhealthy", "error", e.getMessage()))
                .build();
        }
    }
}