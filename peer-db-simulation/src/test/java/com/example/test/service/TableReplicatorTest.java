// src/test/java/com/example/service/TableReplicatorTest.java
package com.example.service;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import jakarta.inject.Inject;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TableReplicatorTest {
    
    @Inject
    TableReplicator tableReplicator;
    
    @Inject
    StateService stateService;
    
    @Inject
    StatusService statusService;
    
    @Inject
    @com.example.qualifier.SinkDatabase
    java.sql.Connection sinkConnection;
    
    @BeforeEach
    void setUp() throws Exception {
        // Clean up before each test
        try (Statement stmt = sinkConnection.createStatement()) {
            stmt.execute("DELETE FROM replication_state");
            stmt.execute("DELETE FROM migration_status");
        }
        stateService.initializeStateTable();
        statusService.initializeMigrationTable();
    }
    
    @Test
    @Order(1)
    void testTableReplicatorInitialization() {
        assertNotNull(tableReplicator);
        assertNotNull(stateService);
        assertNotNull(statusService);
    }
    
    @Test
    @Order(2)
    void testBuildResumableQuery_SingleOrderColumn() {
        String query = tableReplicator.buildResumableQuery(createMockTableConfig("updated_at"), 
            Timestamp.from(Instant.now()), null, 0);
        
        assertNotNull(query);
        assertTrue(query.contains("ORDER BY"));
        assertTrue(query.contains("LIMIT"));
    }
    
    @Test
    @Order(3)
    void testBuildResumableQuery_MultipleOrderColumns() {
        String query = tableReplicator.buildResumableQuery(createMockTableConfig("updated_at, id"),
            Timestamp.from(Instant.now()), "last_id", 1);
        
        assertNotNull(query);
        assertTrue(query.contains("ORDER BY"));
        assertTrue(query.contains("LIMIT"));
    }
    
    @Test
    @Order(4)
    void testReplicationResultRecord() {
        var result = new TableReplicator.ReplicationResult("test_table", 1000L, 5000L, true);
        
        assertEquals("test_table", result.tableName());
        assertEquals(1000L, result.rowsProcessed());
        assertEquals(5000L, result.durationMs());
        assertTrue(result.success());
    }
    
    @Test
    @Order(5)
    void testStateServiceIntegration() {
        stateService.initializeStateTable();
        
        var state = stateService.getState("test_table");
        assertNotNull(state);
        
        // Update state and verify
        stateService.updateState("test_table", 
            Timestamp.from(Instant.now()), "test_id", 100L);
        
        // Verify by reading from service (cache should be consistent)
        var updatedState = stateService.getState("test_table");
        assertTrue(updatedState.hasPreviousState());
    }
    
    @Test
    @Order(6)
    void testStatusServiceIntegration() {
        statusService.initializeMigrationTable();
        statusService.startReplication();
        
        statusService.updateBatchStatus("test_table", "batch1", 50,
            Instant.now().minusSeconds(60),
            Instant.now(),
            "COMPLETED", null);
        
        var status = statusService.getCurrentStatus();
        assertNotNull(status);
        assertEquals(50L, status.totalRecordsMigrated());
    }
    
    private com.example.config.ReplicationConfig.TableConfig createMockTableConfig(String orderBy) {
        return new com.example.config.ReplicationConfig.TableConfig() {
            @Override
            public String sourceQuery() {
                return "SELECT * FROM test_table WHERE updated_at > ?";
            }
            
            @Override
            public int batchSize() {
                return 1000;
            }
            
            @Override
            public String uniqueKey() {
                return "id";
            }
            
            @Override
            public String orderBy() {
                return orderBy;
            }
            
            @Override
            public int virtualThreads() {
                return 2;
            }
        };
    }
}