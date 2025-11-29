// src/main/java/com/example/service/ReplicationOrchestrator.java
package com.example.service;

import com.example.config.ReplicationConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class ReplicationOrchestrator {
    private static final Logger LOG = Logger.getLogger(ReplicationOrchestrator.class);
    
    @Inject
    ReplicationConfig replicationConfig;
    
    @Inject
    TableReplicator tableReplicator;
    
    @Inject
    StateService stateService;
    
    private final ExecutorService virtualThreadExecutor = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().name("orchestrator-", 0).factory()
    );
    
    public ReplicationSummary replicateAllTables() {
        LOG.info("Starting replication for all tables");
        long startTime = System.currentTimeMillis();
        
        stateService.initializeStateTable();
        
        List<Future<TableReplicator.ReplicationResult>> futures = new ArrayList<>();
        var tables = replicationConfig.tables();
        
        // Submit all table replications to virtual threads
        for (var entry : tables.entrySet()) {
            String tableName = entry.getKey();
            var tableConfig = entry.getValue();
            
            Future<TableReplicator.ReplicationResult> future = virtualThreadExecutor.submit(() ->
                tableReplicator.replicateTable(tableName, tableConfig)
            );
            futures.add(future);
        }
        
        // Collect results
        List<TableReplicator.ReplicationResult> results = new ArrayList<>();
        AtomicLong totalRows = new AtomicLong();
        
        for (var future : futures) {
            try {
                var result = future.get();
                results.add(result);
                totalRows.addAndGet(result.rowsProcessed());
            } catch (Exception e) {
                LOG.error("Failed to get replication result", e);
            }
        }
        
        long totalDuration = System.currentTimeMillis() - startTime;
        var summary = new ReplicationSummary(results, totalRows.get(), totalDuration);
        
        logSummary(summary);
        return summary;
    }
    
    public PerformanceEstimate estimatePerformance() {
        int totalVirtualThreads = replicationConfig.tables().values().stream()
            .mapToInt(ReplicationConfig.TableConfig::virtualThreads)
            .sum();
        
        // Conservative estimate: 2000 rows/second per virtual thread
        long estimatedRowsPerSecond = totalVirtualThreads * 2000L;
        long targetRows = 20_000_000L;
        long estimatedSeconds = targetRows / estimatedRowsPerSecond;
        
        return new PerformanceEstimate(totalVirtualThreads, estimatedRowsPerSecond, estimatedSeconds);
    }
    
    private void logSummary(ReplicationSummary summary) {
        LOG.infof("Replication completed in %d ms", summary.totalDurationMs());
        LOG.infof("Total rows processed: %,d", summary.totalRows());
        LOG.infof("Overall throughput: %,d rows/second", 
                 summary.totalRows() / (summary.totalDurationMs() / 1000));
        
        summary.results().forEach(result -> 
            LOG.infof("Table %s: %,d rows in %d ms (%s)", 
                     result.tableName(), result.rowsProcessed(), result.durationMs(),
                     result.success() ? "SUCCESS" : "FAILED")
        );
    }
    
    public record ReplicationSummary(List<TableReplicator.ReplicationResult> results, 
                                   long totalRows, long totalDurationMs) {}
    
    public record PerformanceEstimate(int totalVirtualThreads, long estimatedRowsPerSecond, 
                                    long estimatedSeconds) {}
}