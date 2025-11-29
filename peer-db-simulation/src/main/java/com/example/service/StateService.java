// src/main/java/com/example/service/StateService.java
package com.example.service;

import com.example.qualifier.SinkDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class StateService {
    private static final Logger LOG = Logger.getLogger(StateService.class);
    
    @Inject
    @SinkDatabase
    Connection sinkConnection;
    
    private final Map<String, ReplicationState> stateCache = new ConcurrentHashMap<>();
    
    public void initializeStateTable() {
        try (var statement = sinkConnection.createStatement()) {
            statement.execute("""
                CREATE TABLE IF NOT EXISTS replication_state (
                    table_name VARCHAR(255) PRIMARY KEY,
                    last_sync_timestamp TIMESTAMP,
                    last_sync_id VARCHAR(255),
                    rows_processed BIGINT DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """);
            LOG.info("Replication state table initialized");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize state table", e);
        }
    }
    
    public ReplicationState getState(String tableName) {
        return stateCache.computeIfAbsent(tableName, this::loadStateFromDatabase);
    }
    
    private ReplicationState loadStateFromDatabase(String tableName) {
        String sql = "SELECT last_sync_timestamp, last_sync_id, rows_processed FROM replication_state WHERE table_name = ?";
        
        try (var statement = sinkConnection.prepareStatement(sql)) {
            statement.setString(1, tableName);
            var resultSet = statement.executeQuery();
            
            if (resultSet.next()) {
                return new ReplicationState(
                    resultSet.getTimestamp("last_sync_timestamp"),
                    resultSet.getString("last_sync_id"),
                    resultSet.getLong("rows_processed")
                );
            }
        } catch (SQLException e) {
            LOG.warnf("Failed to load state for table %s: %s", tableName, e.getMessage());
        }
        
        return new ReplicationState(null, null, 0L);
    }
    
    public void updateState(String tableName, Timestamp syncTimestamp, String syncId, long rowsProcessed) {
        // H2 compatible UPSERT using MERGE
        String sql = """
            MERGE INTO replication_state 
            USING (VALUES (?, ?, ?, ?)) AS new_vals(table_name, last_sync_timestamp, last_sync_id, rows_processed)
            ON replication_state.table_name = new_vals.table_name
            WHEN MATCHED THEN
                UPDATE SET last_sync_timestamp = new_vals.last_sync_timestamp,
                          last_sync_id = new_vals.last_sync_id,
                          rows_processed = new_vals.rows_processed,
                          last_updated = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (table_name, last_sync_timestamp, last_sync_id, rows_processed)
                VALUES (new_vals.table_name, new_vals.last_sync_timestamp, new_vals.last_sync_id, new_vals.rows_processed)
        """;
        
        try (var statement = sinkConnection.prepareStatement(sql)) {
            statement.setString(1, tableName);
            statement.setTimestamp(2, syncTimestamp);
            statement.setString(3, syncId);
            statement.setLong(4, rowsProcessed);
            statement.executeUpdate();
            
            // Update cache
            stateCache.put(tableName, new ReplicationState(syncTimestamp, syncId, rowsProcessed));
            
        } catch (SQLException e) {
            LOG.errorf("Failed to update state for table %s: %s", tableName, e.getMessage());
            // Fallback to delete + insert for H2
            fallbackUpdateState(tableName, syncTimestamp, syncId, rowsProcessed);
        }
    }
    
    private void fallbackUpdateState(String tableName, Timestamp syncTimestamp, String syncId, long rowsProcessed) {
        try {
            // First try to update
            String updateSql = """
                UPDATE replication_state 
                SET last_sync_timestamp = ?, last_sync_id = ?, rows_processed = ?, last_updated = CURRENT_TIMESTAMP
                WHERE table_name = ?
            """;
            
            try (var statement = sinkConnection.prepareStatement(updateSql)) {
                statement.setTimestamp(1, syncTimestamp);
                statement.setString(2, syncId);
                statement.setLong(3, rowsProcessed);
                statement.setString(4, tableName);
                
                int updated = statement.executeUpdate();
                
                if (updated == 0) {
                    // No rows updated, so insert
                    String insertSql = """
                        INSERT INTO replication_state (table_name, last_sync_timestamp, last_sync_id, rows_processed)
                        VALUES (?, ?, ?, ?)
                    """;
                    
                    try (var insertStatement = sinkConnection.prepareStatement(insertSql)) {
                        insertStatement.setString(1, tableName);
                        insertStatement.setTimestamp(2, syncTimestamp);
                        insertStatement.setString(3, syncId);
                        insertStatement.setLong(4, rowsProcessed);
                        insertStatement.executeUpdate();
                    }
                }
                
                // Update cache
                stateCache.put(tableName, new ReplicationState(syncTimestamp, syncId, rowsProcessed));
            }
        } catch (SQLException e) {
            LOG.errorf("Fallback update also failed for table %s: %s", tableName, e.getMessage());
        }
    }
    
    public static record ReplicationState(Timestamp lastSyncTimestamp, String lastSyncId, long rowsProcessed) {
        public boolean hasPreviousState() {
            return lastSyncTimestamp != null;
        }
    }
}