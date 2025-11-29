// src/main/java/com/example/config/ReplicationConfig.java
package com.example.config;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "quarkus.replicator")
public interface ReplicationConfig {
    
    Map<String, TableConfig> tables();
    
    @WithDefault("100000")
    int checkpointInterval();
    
    @WithDefault("50000")
    int syncBatchSize();
    
    @WithDefault("50")
    int maxOverallThreads();

    interface TableConfig {
        String sourceQuery();
        
        @WithDefault("10000")
        int batchSize();
        
        String uniqueKey();
        
        String orderBy();
        
        @WithDefault("4")
        int virtualThreads();
    }
}