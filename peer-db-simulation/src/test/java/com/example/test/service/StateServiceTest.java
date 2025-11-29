// src/test/java/com/example/service/StateServiceTest.java
package com.example.service;

import com.example.qualifier.SinkDatabase;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import jakarta.inject.Inject;
import java.sql.*;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StateServiceTest {
    
    @Inject
    StateService stateService;
    
    @Inject
    @SinkDatabase
    Connection sinkConnection;
    
    @BeforeEach
    void setUp() throws SQLException {
        // Clean up before each test
        try (Statement stmt = sinkConnection.createStatement()) {
            stmt.execute("DELETE FROM replication_state");
        }
        stateService.initializeStateTable();
    }
    
    @Test
    @Order(1)
    void testInitializeStateTable() {
        assertDoesNotThrow(() -> stateService.initializeStateTable());
    }
    
    @Test
    @Order(2)
    void testGetState_WhenNoStateExists() {
        StateService.ReplicationState state = stateService.getState("non_existent_table");
        
        assertNotNull(state);
        assertNull(state.lastSyncTimestamp());
        assertNull(state.lastSyncId());
        assertEquals(0L, state.rowsProcessed());
        assertFalse(state.hasPreviousState());
    }
    
    @Test
@Order(3)
void testUpdateAndGetState() {
    String tableName = "test_table";
    Timestamp testTimestamp = Timestamp.from(Instant.now());
    String testId = "test_id_123";
    long rowsProcessed = 1000L;
    
    // Update state
    stateService.updateState(tableName, testTimestamp, testId, rowsProcessed);
    
    // Retrieve state from database directly to verify
    StateService.ReplicationState state = readStateDirectlyFromDatabase(tableName);
    
    assertNotNull(state);
    // Compare timestamps with tolerance for nanosecond differences
    assertTrue(Math.abs(testTimestamp.getTime() - state.lastSyncTimestamp().getTime()) < 1000,
        "Timestamps should be within 1 second of each other");
    assertEquals(testId, state.lastSyncId());
    assertEquals(rowsProcessed, state.rowsProcessed());
    assertTrue(state.hasPreviousState());
}

@Test
@Order(4)
void testUpdateState_MultipleUpdates() {
    String tableName = "multi_update_table";
    Timestamp firstTimestamp = Timestamp.from(Instant.now().minusSeconds(3600));
    Timestamp secondTimestamp = Timestamp.from(Instant.now());
    
    // First update
    stateService.updateState(tableName, firstTimestamp, "id1", 500L);
    
    // Second update
    stateService.updateState(tableName, secondTimestamp, "id2", 1500L);
    
    // Read from database directly
    StateService.ReplicationState state = readStateDirectlyFromDatabase(tableName);
    
    // Compare timestamps with tolerance
    assertTrue(Math.abs(secondTimestamp.getTime() - state.lastSyncTimestamp().getTime()) < 1000,
        "Timestamps should be within 1 second of each other");
    assertEquals("id2", state.lastSyncId());
    assertEquals(1500L, state.rowsProcessed());
}


    @Test
    @Order(5)
    void testStateCacheConsistency() {
        String tableName = "cached_table";
        Timestamp timestamp = Timestamp.from(Instant.now());
        
        // Update and get should be consistent within same instance
        stateService.updateState(tableName, timestamp, "cache_test", 999L);
        
        StateService.ReplicationState firstGet = stateService.getState(tableName);
        StateService.ReplicationState secondGet = stateService.getState(tableName);
        
        assertEquals(firstGet.lastSyncTimestamp(), secondGet.lastSyncTimestamp());
        assertEquals(firstGet.lastSyncId(), secondGet.lastSyncId());
        assertEquals(firstGet.rowsProcessed(), secondGet.rowsProcessed());
    }
    
    @Test
    @Order(6)
    void testMultipleTablesState() {
        String[] tableNames = {"table1", "table2", "table3"};
        
        for (int i = 0; i < tableNames.length; i++) {
            stateService.updateState(
                tableNames[i], 
                Timestamp.from(Instant.now().minusSeconds(i * 1000L)),
                "id_" + i,
                (i + 1) * 1000L
            );
        }
        
        // Read all states from database directly
        for (int i = 0; i < tableNames.length; i++) {
            StateService.ReplicationState state = readStateDirectlyFromDatabase(tableNames[i]);
            assertEquals((i + 1) * 1000L, state.rowsProcessed(), 
                "Table " + tableNames[i] + " should have " + ((i + 1) * 1000L) + " rows processed");
            assertEquals("id_" + i, state.lastSyncId());
        }
    }
    // Helper method to read state directly from database, bypassing cache
    private StateService.ReplicationState readStateDirectlyFromDatabase(String tableName) {
        String sql = "SELECT last_sync_timestamp, last_sync_id, rows_processed FROM replication_state WHERE table_name = ?";
        
        try (var statement = sinkConnection.prepareStatement(sql)) {
            statement.setString(1, tableName);
            var resultSet = statement.executeQuery();
            
            if (resultSet.next()) {
                return new StateService.ReplicationState(
                    resultSet.getTimestamp("last_sync_timestamp"),
                    resultSet.getString("last_sync_id"),
                    resultSet.getLong("rows_processed")
                );
            }
        } catch (SQLException e) {
            fail("Failed to read state from database: " + e.getMessage());
        }
        
        return new StateService.ReplicationState(null, null, 0L);
    }
}