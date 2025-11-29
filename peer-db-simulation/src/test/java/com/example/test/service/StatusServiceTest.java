// src/test/java/com/example/service/StatusServiceTest.java
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
class StatusServiceTest {
    
    @Inject
    StatusService statusService;
    
    @Inject
    @SinkDatabase
    java.sql.Connection sinkConnection;
    
    @BeforeEach
    void setUp() throws Exception {
        // Clean up before each test
        try (Statement stmt = sinkConnection.createStatement()) {
            stmt.execute("DELETE FROM migration_status");
        }
        statusService.initializeMigrationTable();
    }
    
    @Test
    @Order(1)
    void testInitializeMigrationTable() {
        assertDoesNotThrow(() -> statusService.initializeMigrationTable());
    }
    
    @Test
    @Order(2)
    void testStartAndEndReplication() {
        statusService.startReplication();
        
        StatusService.MigrationStatus status = statusService.getCurrentStatus();
        assertNotNull(status.startTime());
        assertNull(status.endTime());
        assertEquals(0L, status.totalRecordsMigrated());
        
        statusService.endReplication();
        
        StatusService.MigrationStatus finalStatus = statusService.getCurrentStatus();
        assertNotNull(finalStatus.endTime());
    }
    
    @Test
    @Order(3)
    void testUpdateBatchStatus() {
        statusService.startReplication();
        
        String tableName = "test_table";
        String batchId = "batch_123";
        int recordsMigrated = 150;
        Instant batchStart = Instant.now().minusSeconds(60);
        Instant batchEnd = Instant.now();
        
        statusService.updateBatchStatus(
            tableName, batchId, recordsMigrated, 
            batchStart, batchEnd, "COMPLETED", null
        );
        
        // Verify in database directly
        int dbRecords = getTotalRecordsFromDatabase();
        assertEquals(recordsMigrated, dbRecords, "Database should have the correct number of records");
        
        StatusService.MigrationStatus status = statusService.getCurrentStatus();
        assertEquals(recordsMigrated, status.totalRecordsMigrated());
        
        StatusService.TableStatus tableStatus = status.tableStatuses().get(0);
        assertEquals(tableName, tableStatus.tableName());
        assertEquals(recordsMigrated, tableStatus.recordsMigrated());
        assertEquals(1, tableStatus.successfulBatches());
        assertEquals(0, tableStatus.failedBatches());
    }
    // @Test
    // @Order(4)
    // void testUpdateErrorStatus() {
    //     statusService.startReplication();
        
    //     String tableName = "error_table";
    //     String batchId = "batch_error_456";
    //     String errorMessage = "Connection timeout";
        
    //     // First, let's verify initial state
    //     StatusService.MigrationStatus initialStatus = statusService.getCurrentStatus();
    //     assertTrue(initialStatus.tableStatuses().isEmpty(), "Should start with no table statuses");
        
    //     statusService.updateErrorStatus(tableName, batchId, errorMessage);
        
    //     StatusService.MigrationStatus status = statusService.getCurrentStatus();
        
    //     // Should have one table status now
    //     assertEquals(1, status.tableStatuses().size(), "Should have one table status after error");
        
    //     StatusService.TableStatus tableStatus = status.tableStatuses().get(0);
    //     assertEquals(tableName, tableStatus.tableName());
    //     assertEquals(0, tableStatus.recordsMigrated(), "Should have 0 records migrated for error");
    //     assertEquals(0, tableStatus.successfulBatches(), "Should have 0 successful batches for error");
    //     assertEquals(1, tableStatus.failedBatches(), "Should have 1 failed batch after error");
    // }
    
    @Test
    @Order(5)
    void testMultipleBatchesSameTable() {
        statusService.startReplication();
        
        String tableName = "multi_batch_table";
        
        // Add multiple batches
        for (int i = 1; i <= 5; i++) {
            statusService.updateBatchStatus(
                tableName,
                "batch_" + i,
                i * 100,
                Instant.now().minusSeconds(300 - i * 60),
                Instant.now().minusSeconds(300 - i * 60 + 30),
                "COMPLETED",
                null
            );
        }
        
        // Verify total in database
        int totalRecords = getTotalRecordsFromDatabase();
        assertEquals(1500, totalRecords, "Database should have 1500 total records");
        
        StatusService.MigrationStatus status = statusService.getCurrentStatus();
        StatusService.TableStatus tableStatus = status.tableStatuses().get(0);
        assertEquals(1500L, tableStatus.recordsMigrated());
        assertEquals(5, tableStatus.successfulBatches());
        assertEquals(0, tableStatus.failedBatches());
    }
    
    @Test
    @Order(6)
    void testGetDetailedStatus() {
        statusService.startReplication();
        
        // Add some test data
        statusService.updateBatchStatus("table1", "batch1", 100, 
            Instant.now().minusSeconds(300), Instant.now().minusSeconds(270), "COMPLETED", null);
        statusService.updateBatchStatus("table2", "batch2", 200, 
            Instant.now().minusSeconds(240), Instant.now().minusSeconds(210), "COMPLETED", null);
        
        // Verify database has the records
        int totalRecords = getTotalRecordsFromDatabase();
        assertEquals(300, totalRecords, "Database should have 300 total records");
        
        StatusService.DetailedMigrationStatus detailedStatus = statusService.getDetailedStatus();
        assertNotNull(detailedStatus);
        assertEquals(300L, detailedStatus.currentStatus().totalRecordsMigrated());
    }
    
    // @Test
    // @Order(7)
    // void testThroughputCalculation() {
    //     // Create a fresh status service instance for this test
    //     StatusService testService = new StatusService();
    //     testService.initializeMigrationTable();
        
    //     // Set a fixed start time for predictable calculation
    //     Instant startTime = Instant.now().minusSeconds(100);
    //     testService.startReplication();
        
    //     // Use reflection to set the start time for testing
    //     try {
    //         java.lang.reflect.Field startTimeField = StatusService.class.getDeclaredField("replicationStartTime");
    //         startTimeField.setAccessible(true);
    //         startTimeField.set(testService, startTime);
    //     } catch (Exception e) {
    //         fail("Failed to set start time for testing: " + e.getMessage());
    //     }
        
    //     // Add records with known timing
    //     testService.updateBatchStatus("throughput_table", "batch1", 50000, 
    //         startTime, startTime.plusSeconds(50), "COMPLETED", null);
    //     testService.updateBatchStatus("throughput_table", "batch2", 50000, 
    //         startTime.plusSeconds(50), startTime.plusSeconds(100), "COMPLETED", null);
        
    //     StatusService.MigrationStatus status = testService.getCurrentStatus();
    //     StatusService.ThroughputMetrics throughput = status.throughput();
        
    //     assertNotNull(throughput);
        
    //     // With 100000 records over 100 seconds, should be exactly 1000 records/second
    //     assertEquals(1000L, throughput.recordsPerSecond(), 
    //         "Should calculate 1000 records/second for 100000 records over 100 seconds");
    //     assertEquals(60000L, throughput.recordsPerMinute());
    //     assertEquals(3600000L, throughput.recordsPerHour());
    // }
    
    @Test
    @Order(8)
    void testTableDetailedStatus() {
        statusService.startReplication();
        
        String tableName = "detailed_table";
        statusService.updateBatchStatus(tableName, "batch1", 750, 
            Instant.now().minusSeconds(600), Instant.now().minusSeconds(570), "COMPLETED", null);
        statusService.updateBatchStatus(tableName, "batch2", 250, 
            Instant.now().minusSeconds(540), Instant.now().minusSeconds(510), "COMPLETED", null);
        
        StatusService.TableDetailedStatus tableStatus = statusService.getTableDetailedStatus(tableName);
        
        assertNotNull(tableStatus);
        assertEquals(tableName, tableStatus.tableName());
        assertEquals(1000L, tableStatus.recordsMigrated());
        assertEquals(2, tableStatus.totalBatches());
        assertEquals(0, tableStatus.failedBatches());
    }
    
    // Helper methods to read directly from database with proper exception handling
    private int getTotalRecordsFromDatabase() {
        String sql = "SELECT SUM(records_migrated) as total FROM migration_status";
        try (var statement = sinkConnection.createStatement();
             var resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return resultSet.getInt("total");
            }
        } catch (SQLException e) {
            fail("Failed to get total records from database: " + e.getMessage());
        }
        return 0;
    }
    
    private int getFailedBatchesFromDatabase() {
        String sql = "SELECT COUNT(*) as failed FROM migration_status WHERE status = 'FAILED'";
        try (var statement = sinkConnection.createStatement();
             var resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return resultSet.getInt("failed");
            }
        } catch (SQLException e) {
            fail("Failed to get failed batches from database: " + e.getMessage());
        }
        return 0;
    }
}