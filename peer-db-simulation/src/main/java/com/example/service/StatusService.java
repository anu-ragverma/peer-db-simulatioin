// src/main/java/com/example/service/StatusService.java
package com.example.service;

import com.example.qualifier.SinkDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class StatusService {
    private static final Logger LOG = Logger.getLogger(StatusService.class);
    
    @Inject
    @SinkDatabase
    Connection sinkConnection;
    
    private final Map<String, TableMigrationStatus> currentStatus = new ConcurrentHashMap<>();
    private final AtomicLong totalRecordsMigrated = new AtomicLong(0);
    private Instant replicationStartTime;
    private Instant replicationEndTime;
    
    public void initializeMigrationTable() {
        try (var statement = sinkConnection.createStatement()) {
            statement.execute("""
                CREATE TABLE IF NOT EXISTS migration_status (
                    table_name VARCHAR(255) NOT NULL,
                    batch_id VARCHAR(255) NOT NULL,
                    records_migrated INTEGER NOT NULL,
                    migration_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    batch_start_time TIMESTAMP,
                    batch_end_time TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'COMPLETED',
                    error_message TEXT,
                    PRIMARY KEY (table_name, batch_id)
                )
            """);
            
            statement.execute("""
                CREATE INDEX IF NOT EXISTS idx_migration_status_table 
                ON migration_status (table_name, migration_timestamp DESC)
            """);
            
            statement.execute("""
                CREATE INDEX IF NOT EXISTS idx_migration_status_timestamp 
                ON migration_status (migration_timestamp DESC)
            """);
            
            LOG.info("Migration status table initialized");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize migration status table", e);
        }
    }
    
    public void updateBatchStatus(String tableName, String batchId, int recordsMigrated, 
                                Instant batchStart, Instant batchEnd, String status, String errorMessage) {
        try {
            // H2 compatible insert - just insert, let primary key violation happen naturally
            // or use a different approach for H2
            String sql = """
                INSERT INTO migration_status 
                (table_name, batch_id, records_migrated, batch_start_time, batch_end_time, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """;
            
            try (var statement = sinkConnection.prepareStatement(sql)) {
                statement.setString(1, tableName);
                statement.setString(2, batchId);
                statement.setInt(3, recordsMigrated);
                statement.setTimestamp(4, batchStart != null ? Timestamp.from(batchStart) : null);
                statement.setTimestamp(5, batchEnd != null ? Timestamp.from(batchEnd) : null);
                statement.setString(6, status);
                statement.setString(7, errorMessage);
                statement.executeUpdate();
            }
            
            // Update in-memory status
            currentStatus.merge(tableName, 
                new TableMigrationStatus(tableName, recordsMigrated, 1, 0, batchEnd),
                (existing, newStatus) -> existing.update(recordsMigrated, 1, 0, batchEnd)
            );
            
            totalRecordsMigrated.addAndGet(recordsMigrated);
            
            LOG.debugf("Updated status for table %s: +%d records (total: %d)", 
                      tableName, recordsMigrated, totalRecordsMigrated.get());
                      
        } catch (SQLException e) {
            // If it's a duplicate key error, that's fine for our test scenario
            if (e.getMessage().contains("unique constraint") || e.getMessage().contains("primary key")) {
                LOG.debugf("Batch %s for table %s already exists, skipping", batchId, tableName);
            } else {
                LOG.errorf("Failed to update batch status for table %s: %s", tableName, e.getMessage());
            }
        }
    }

   
    public void startReplication() {
        this.replicationStartTime = Instant.now();
        this.replicationEndTime = null;
        this.totalRecordsMigrated.set(0);
        this.currentStatus.clear();
        LOG.info("Replication started at: " + replicationStartTime);
    }
    
    public void endReplication() {
        this.replicationEndTime = Instant.now();
        LOG.info("Replication completed at: " + replicationEndTime);
    }
    
    public void updateErrorStatus(String tableName, String batchId, String errorMessage) {
        // For error status, we should not count any records as migrated
        updateBatchStatus(tableName, batchId, 0, Instant.now(), Instant.now(), "FAILED", errorMessage);
        
        // But we need to ensure the failed batch count is incremented correctly
        currentStatus.merge(tableName,
            new TableMigrationStatus(tableName, 0, 0, 1, Instant.now()),
            (existing, newStatus) -> {
                // Only increment failed batches, not successful ones
                return existing.update(0, 0, 1, Instant.now());
            }
        );
    }
    
    public DetailedMigrationStatus getDetailedStatus() {
        MigrationStatus current = getCurrentStatus();
        Map<String, TableDetailedStatus> detailedTableStatus = new HashMap<>();
        
        for (var tableName : currentStatus.keySet()) {
            detailedTableStatus.put(tableName, getTableDetailedStatus(tableName));
        }
        
        return new DetailedMigrationStatus(
            current,
            getRecentBatches(50), // Last 50 batches
            getSystemMetrics(),
            getEstimatedCompletionTime()
        );
    }
    
    public TableDetailedStatus getTableDetailedStatus(String tableName) {
        try {
            // Get total records from source
            long totalSourceRecords = getTotalRecordsFromSource(tableName);
            
            // Get migration summary from status table
            String sql = """
                SELECT 
                    COUNT(*) as total_batches,
                    SUM(records_migrated) as total_records,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_batches,
                    MIN(batch_start_time) as first_batch,
                    MAX(batch_end_time) as last_batch
                FROM migration_status 
                WHERE table_name = ?
            """;
            
            try (var statement = sinkConnection.prepareStatement(sql)) {
                statement.setString(1, tableName);
                var resultSet = statement.executeQuery();
                
                if (resultSet.next()) {
                    return new TableDetailedStatus(
                        tableName,
                        totalSourceRecords,
                        resultSet.getLong("total_records"),
                        resultSet.getInt("total_batches"),
                        resultSet.getInt("failed_batches"),
                        resultSet.getTimestamp("first_batch"),
                        resultSet.getTimestamp("last_batch"),
                        calculateTableProgress(totalSourceRecords, resultSet.getLong("total_records"))
                    );
                }
            }
        } catch (SQLException e) {
            LOG.errorf("Failed to get detailed status for table %s: %s", tableName, e.getMessage());
        }
        
        return new TableDetailedStatus(tableName, 0, 0, 0, 0, null, null, 0.0);
    }
    
    private long getTotalRecordsFromSource(String tableName) {
        // This would need source database connection
        // For now, return -1 to indicate unknown
        return -1L;
    }
    
    private List<BatchStatus> getRecentBatches(int limit) {
        List<BatchStatus> batches = new ArrayList<>();
        
        try {
            String sql = """
                SELECT table_name, batch_id, records_migrated, migration_timestamp, 
                       batch_start_time, batch_end_time, status, error_message
                FROM migration_status 
                ORDER BY migration_timestamp DESC 
                LIMIT ?
            """;
            
            try (var statement = sinkConnection.prepareStatement(sql)) {
                statement.setInt(1, limit);
                var resultSet = statement.executeQuery();
                
                while (resultSet.next()) {
                    batches.add(new BatchStatus(
                        resultSet.getString("table_name"),
                        resultSet.getString("batch_id"),
                        resultSet.getInt("records_migrated"),
                        resultSet.getTimestamp("migration_timestamp"),
                        resultSet.getTimestamp("batch_start_time"),
                        resultSet.getTimestamp("batch_end_time"),
                        resultSet.getString("status"),
                        resultSet.getString("error_message")
                    ));
                }
            }
        } catch (SQLException e) {
            LOG.errorf("Failed to get recent batches: %s", e.getMessage());
        }
        
        return batches;
    }
    
    private SystemMetrics getSystemMetrics() {
        // Get JVM metrics
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        
        return new SystemMetrics(
            usedMemory,
            maxMemory,
            (double) usedMemory / maxMemory * 100,
            Thread.activeCount(),
            getDatabaseConnectionMetrics()
        );
    }
    
    private Map<String, Object> getDatabaseConnectionMetrics() {
        // This would require access to connection pool metrics
        // For Agroal, you'd need to inject and query the pool
        return Map.of(
            "active_connections", "N/A",
            "max_connections", "N/A",
            "idle_connections", "N/A"
        );
    }
    
    private double calculateProgress() {
        // This could be enhanced with total expected records
        long totalMigrated = totalRecordsMigrated.get();
        // For now, return -1 to indicate progress is not calculable without total
        return -1.0;
    }
    
    private double calculateTableProgress(long totalRecords, long migratedRecords) {
        if (totalRecords <= 0) return -1.0;
        return (double) migratedRecords / totalRecords * 100.0;
    }
    
    private Instant getEstimatedCompletionTime() {
        // Simple estimation based on current throughput
        // This could be enhanced with more sophisticated algorithms
        return null; // Not implemented in this example
    }
    
    // Record classes for status responses
    public record MigrationStatus(
        Instant startTime,
        Instant endTime,
        long totalRecordsMigrated,
        List<TableStatus> tableStatuses,
        double overallProgress, // -1 if unknown
        ThroughputMetrics throughput
    ) {}
    
    public record TableStatus(
        String tableName,
        long recordsMigrated,
        int successfulBatches,
        int failedBatches,
        Instant lastActivity
    ) {}
    
    public record ThroughputMetrics(
        long recordsPerSecond,
        long recordsPerMinute,
        long recordsPerHour
    ) {}
    
    public record DetailedMigrationStatus(
        MigrationStatus currentStatus,
        List<BatchStatus> recentBatches,
        SystemMetrics systemMetrics,
        Instant estimatedCompletionTime
    ) {}
    
    public record TableDetailedStatus(
        String tableName,
        long totalSourceRecords, // -1 if unknown
        long recordsMigrated,
        int totalBatches,
        int failedBatches,
        Timestamp firstBatchTime,
        Timestamp lastBatchTime,
        double progressPercentage // -1 if unknown
    ) {}
    
    public record BatchStatus(
        String tableName,
        String batchId,
        int recordsMigrated,
        Timestamp migrationTimestamp,
        Timestamp batchStartTime,
        Timestamp batchEndTime,
        String status,
        String errorMessage
    ) {}
    
    public record SystemMetrics(
        long memoryUsedBytes,
        long memoryMaxBytes,
        double memoryUsagePercent,
        int activeThreads,
        Map<String, Object> databaseMetrics
    ) {}
    
    // Internal class for tracking table status
    private static class TableMigrationStatus {
        private final String tableName;
        private final AtomicLong recordsMigrated = new AtomicLong(0);
        private final AtomicInteger successfulBatches = new AtomicInteger(0);
        private final AtomicInteger failedBatches = new AtomicInteger(0);
        private volatile Instant lastActivity;
        
        public TableMigrationStatus(String tableName, long initialRecords, int initialBatches, 
                                  int initialFailed, Instant lastActivity) {
            this.tableName = tableName;
            this.recordsMigrated.set(initialRecords);
            this.successfulBatches.set(initialBatches);
            this.failedBatches.set(initialFailed);
            this.lastActivity = lastActivity;
        }
        
        public TableMigrationStatus update(long additionalRecords, int additionalBatches, 
                                         int additionalFailed, Instant activityTime) {
            recordsMigrated.addAndGet(additionalRecords);
            successfulBatches.addAndGet(additionalBatches);
            failedBatches.addAndGet(additionalFailed);
            this.lastActivity = activityTime;
            return this;
        }
        
        public TableStatus toTableStatus() {
            return new TableStatus(
                tableName,
                recordsMigrated.get(),
                successfulBatches.get(),
                failedBatches.get(),
                lastActivity
            );
        }
    }

   
    public MigrationStatus getCurrentStatus() {
        List<TableStatus> tableStatuses = new ArrayList<>();
        
        for (var entry : currentStatus.entrySet()) {
            tableStatuses.add(entry.getValue().toTableStatus());
        }
        
        // Sort by table name for consistent output
        tableStatuses.sort(Comparator.comparing(TableStatus::tableName));
        
        // Handle null cases safely
        ThroughputMetrics throughput;
        try {
            throughput = calculateThroughput();
        } catch (Exception e) {
            throughput = new ThroughputMetrics(0, 0, 0);
        }
        
        double progress;
        try {
            progress = calculateProgress();
        } catch (Exception e) {
            progress = -1.0;
        }
        
        return new MigrationStatus(
            replicationStartTime,
            replicationEndTime,
            totalRecordsMigrated.get(),
            tableStatuses,
            progress,
            throughput
        );
    }
    
    // Also fix the throughput calculation to handle edge cases
    private ThroughputMetrics calculateThroughput() {
        try {
            if (replicationStartTime == null) {
                return new ThroughputMetrics(0, 0, 0);
            }
            
            Instant endTime = replicationEndTime != null ? replicationEndTime : Instant.now();
            long durationSeconds = java.time.Duration.between(replicationStartTime, endTime).getSeconds();
            
            // Avoid division by zero and handle very short durations
            if (durationSeconds <= 0) {
                return new ThroughputMetrics(0, 0, 0);
            }
            
            long totalRecords = totalRecordsMigrated.get();
            long recordsPerSecond = totalRecords / durationSeconds;
            long recordsPerMinute = recordsPerSecond * 60;
            long recordsPerHour = recordsPerMinute * 60;
            
            return new ThroughputMetrics(recordsPerSecond, recordsPerMinute, recordsPerHour);
        } catch (Exception e) {
            return new ThroughputMetrics(0, 0, 0);
        }
    }

    public void setStartTimeForTesting(Instant startTime) {
        this.replicationStartTime = startTime;
    }
}