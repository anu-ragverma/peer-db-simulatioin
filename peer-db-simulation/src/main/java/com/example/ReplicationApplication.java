// src/main/java/com/example/ReplicationApplication.java
package com.example;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;

import com.example.service.ReplicationOrchestrator;

@QuarkusMain
public class ReplicationApplication implements QuarkusApplication {
    
    @Inject
    ReplicationOrchestrator orchestrator;
    
    @Override
    public int run(String... args) throws Exception {
        if (args.length > 0 && "run".equals(args[0])) {
            System.out.println("Starting PostgreSQL Replication...");
            
            var estimate = orchestrator.estimatePerformance();
            System.out.printf("Performance Estimate:%n");
            System.out.printf("  Virtual Threads: %d%n", estimate.totalVirtualThreads());
            System.out.printf("  Estimated Rows/Sec: %,d%n", estimate.estimatedRowsPerSecond());
            System.out.printf("  Estimated Time for 20M rows: %d seconds%n", estimate.estimatedSeconds());
            
            var summary = orchestrator.replicateAllTables();
            System.out.printf("Replication Completed:%n");
            System.out.printf("  Total Rows: %,d%n", summary.totalRows());
            System.out.printf("  Total Time: %d ms%n", summary.totalDurationMs());
            
            return summary.results().stream().allMatch(r -> r.success()) ? 0 : 1;
        }
        
        Quarkus.waitForExit();
        return 0;
    }
}