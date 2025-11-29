// src/test/java/com/example/test/ConfigHelper.java
package com.example.test;

import com.example.config.ReplicationConfig;

import java.util.Map;

public class ConfigHelper {
    
    public static ReplicationConfig.TableConfig createTableConfig(String sourceQuery, int batchSize, 
                                                                String uniqueKey, String orderBy, int virtualThreads) {
        return new ReplicationConfig.TableConfig() {
            @Override
            public String sourceQuery() {
                return sourceQuery;
            }
            
            @Override
            public int batchSize() {
                return batchSize;
            }
            
            @Override
            public String uniqueKey() {
                return uniqueKey;
            }
            
            @Override
            public String orderBy() {
                return orderBy;
            }
            
            @Override
            public int virtualThreads() {
                return virtualThreads;
            }
        };
    }
}