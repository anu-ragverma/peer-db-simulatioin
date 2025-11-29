// src/main/java/com/example/service/TableReplicator.java
package com.example.service;

import com.example.config.ReplicationConfig;
import com.example.qualifier.SinkDatabase;
import com.example.qualifier.SourceDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class TableReplicator {
    private static final Logger LOG = Logger.getLogger(TableReplicator.class);
    
    @Inject
    @SourceDatabase
    Connection sourceConnection;
    
    @Inject
    @SinkDatabase
    Connection sinkConnection;
    
    @Inject
    StateService stateService;
    
    @Inject
    StatusService statusService;
    
    @Inject
    ReplicationConfig replicationConfig;
    
    private final ExecutorService virtualThreadExecutor = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().name("replicator-", 0).factory()
    );
    
    public ReplicationResult replicateTable(String tableName, ReplicationConfig.TableConfig tableConfig) {
        LOG.infof("Starting replication for table: %s", tableName);
        long startTime = System.currentTimeMillis();
        
        try {
            ensureSinkTable(tableName);
            var state = stateService.getState(tableName);
            
            long totalRows = replicateWithVirtualThreads(tableName, tableConfig, state);
            long duration = System.currentTimeMillis() - startTime;
            
            LOG.infof("Completed replication for table %s: %d rows in %d ms", 
                     tableName, totalRows, duration);
            
            return new ReplicationResult(tableName, totalRows, duration, true);
            
        } catch (Exception e) {
            String errorMsg = String.format("Replication failed for table %s: %s", tableName, e.getMessage());
            statusService.updateErrorStatus(tableName, "main", errorMsg);
            LOG.errorf("Replication failed for table %s: %s", tableName, e.getMessage());
            return new ReplicationResult(tableName, 0, 0, false);
        }
    }
    
    private long replicateWithVirtualThreads(String tableName, 
                                           ReplicationConfig.TableConfig tableConfig,
                                           StateService.ReplicationState initialState) throws Exception {
        AtomicLong totalRows = new AtomicLong(initialState.rowsProcessed());
        String batchId = "batch_" + Instant.now().getEpochSecond() + "_" + tableName;
        
        Timestamp lastTimestamp = initialState.lastSyncTimestamp();
        String lastId = initialState.lastSyncId();
        
        if (lastTimestamp == null) {
            lastTimestamp = getMaxTimestampFromSource(tableName, tableConfig);
            if (lastTimestamp == null) {
                LOG.infof("No data found for table %s", tableName);
                return 0;
            }
        }
        
        boolean hasMoreData = true;
        int consecutiveEmptyBatches = 0;
        
        while (hasMoreData && consecutiveEmptyBatches < 3) {
            List<Future<Integer>> futures = new ArrayList<>();
            
            // Use virtual threads for parallel batch processing
            for (int i = 0; i < tableConfig.virtualThreads(); i++) {
                final Timestamp currentTimestamp = lastTimestamp;
                final String currentLastId = lastId;
                final int threadNum = i;
                
                Future<Integer> future = virtualThreadExecutor.submit(() -> 
                    processBatch(tableName, tableConfig, currentTimestamp, currentLastId, 
                               batchId + "_" + threadNum, threadNum)
                );
                futures.add(future);
            }
            
            // Wait for all virtual threads to complete
            int batchTotal = 0;
            for (Future<Integer> future : futures) {
                batchTotal += future.get();
            }
            
            if (batchTotal == 0) {
                consecutiveEmptyBatches++;
            } else {
                consecutiveEmptyBatches = 0;
                totalRows.addAndGet(batchTotal);
                
                // Update checkpoint
                var newState = getLastProcessedValues(tableName, tableConfig);
                if (newState.timestamp() != null) {
                    stateService.updateState(tableName, newState.timestamp(), 
                                           newState.lastId(), totalRows.get());
                    lastTimestamp = newState.timestamp();
                    lastId = newState.lastId();
                }
                
                // FIXED: Resolve ambiguous debugf call by using explicit types
                LOG.debugf("Processed %d rows for %s, total: %d", 
                          Integer.valueOf(batchTotal), tableName, Long.valueOf(totalRows.get()));
                
                // Check if we need more batches
                hasMoreData = batchTotal >= (tableConfig.batchSize() * tableConfig.virtualThreads());
            }
        }
        
        return totalRows.get();
    }
    
    private int processBatch(String tableName, ReplicationConfig.TableConfig tableConfig,
                           Timestamp lastTimestamp, String lastId, String batchId, int threadOffset) {
        Instant batchStart = Instant.now();
        int recordsMigrated = 0;
        String status = "COMPLETED";
        String errorMessage = null;
        
        try {
            List<Map<String, Object>> rows = fetchBatch(tableName, tableConfig, lastTimestamp, lastId, threadOffset);
            recordsMigrated = rows.size();
            
            if (!rows.isEmpty()) {
                insertBatch(tableName, rows, batchId, tableConfig.uniqueKey());
            }
            
        } catch (Exception e) {
            status = "FAILED";
            errorMessage = e.getMessage();
            LOG.errorf("Batch failed for table %s: %s", tableName, e.getMessage());
        }
        
        Instant batchEnd = Instant.now();
        
        // Update status
        if ("COMPLETED".equals(status)) {
            statusService.updateBatchStatus(tableName, batchId, recordsMigrated, batchStart, batchEnd, status, errorMessage);
        } else {
            statusService.updateErrorStatus(tableName, batchId, errorMessage);
        }
        
        return recordsMigrated;
    }
    
    private List<Map<String, Object>> fetchBatch(String tableName, ReplicationConfig.TableConfig tableConfig,
        Timestamp lastTimestamp, String lastId, int threadOffset) {
List<Map<String, Object>> rows = new ArrayList<>();
String query = buildResumableQuery(tableConfig, lastTimestamp, lastId, threadOffset);

try (var statement = sourceConnection.prepareStatement(query)) {
int paramIndex = 1;

// Log the query and parameters for debugging
LOG.debugf("Executing query for table %s: %s", tableName, query);
LOG.debugf("Parameters: lastTimestamp=%s, lastId=%s, threadOffset=%d, virtualThreads=%d", 
lastTimestamp, lastId, threadOffset, tableConfig.virtualThreads());

// Always set the timestamp parameter
statement.setTimestamp(paramIndex++, lastTimestamp);

String[] orderColumns = tableConfig.orderBy().split(",");
boolean hasMultipleOrderColumns = (lastId != null && orderColumns.length > 1);

if (hasMultipleOrderColumns) {
// For multiple order columns, we need to set lastId twice in the complex condition
statement.setString(paramIndex++, lastId);  // For the OR condition
statement.setString(paramIndex++, lastId);  // For the AND condition within OR
}

// Set the modulo partitioning parameters
statement.setInt(paramIndex++, tableConfig.virtualThreads()); // modulo divisor
statement.setInt(paramIndex, threadOffset);                   // modulo remainder

LOG.debugf("Set %d parameters for query", paramIndex);

var resultSet = statement.executeQuery();
var metaData = resultSet.getMetaData();

while (resultSet.next()) {
var row = new java.util.HashMap<String, Object>();
for (int i = 1; i <= metaData.getColumnCount(); i++) {
row.put(metaData.getColumnName(i), resultSet.getObject(i));
}
rows.add(row);
}

LOG.debugf("Fetched %d rows for table %s", rows.size(), tableName);

} catch (SQLException e) {
LOG.errorf("Failed to fetch batch for table %s: %s. Query: %s", tableName, e.getMessage(), query);
}

return rows;
}

String buildResumableQuery(ReplicationConfig.TableConfig tableConfig, 
    Timestamp lastTimestamp, String lastId, int threadOffset) {
String baseQuery = tableConfig.sourceQuery();
String orderBy = tableConfig.orderBy();
String[] orderColumns = orderBy.split(",");

StringBuilder query = new StringBuilder(baseQuery);

// Remove any existing WHERE clause from the base query if it's incomplete
if (baseQuery.toUpperCase().contains("WHERE") && !baseQuery.toUpperCase().contains("ORDER BY")) {
// The base query already has a WHERE clause, we need to add to it
query.append(" AND ");
} else if (!baseQuery.toUpperCase().contains("WHERE")) {
// No WHERE clause in base query, add one
query.append(" WHERE ");
}

String timestampColumn = orderColumns[0].trim().split(" ")[0];
boolean hasMultipleOrderColumns = (lastId != null && orderColumns.length > 1);

if (hasMultipleOrderColumns) {
String idColumn = orderColumns[1].trim().split(" ")[0];
// Complex condition for resume with multiple order columns
query.append("(")
.append(timestampColumn).append(" > ? OR (")
.append(timestampColumn).append(" = ? AND ")
.append(idColumn).append(" > ?))");
} else {
// Simple condition for single order column
query.append(timestampColumn).append(" >= ?");
}

// Add modulo-based partitioning for virtual threads
// Using a simple hash function that works across databases
query.append(" AND MOD(ABS(CAST((")
.append(tableConfig.uniqueKey())
.append(") AS BIGINT)), ?) = ?");

query.append(" ORDER BY ").append(orderBy)
.append(" LIMIT ").append(tableConfig.batchSize());

return query.toString();
}
    // private
     void insertBatch(String tableName, List<Map<String, Object>> rows, 
                           String batchId, String uniqueKey) {
        if (rows.isEmpty()) return;
        
        var columns = new ArrayList<>(rows.get(0).keySet());
        columns.add("synced_at");
        columns.add("_replication_batch_id");
        
        String placeholders = String.join(",", java.util.Collections.nCopies(columns.size(), "?"));
        String columnNames = String.join(",", columns);
        
        String insertSql = String.format("""
            INSERT INTO %s (%s) VALUES (%s)
            ON CONFLICT (%s) DO UPDATE SET %s, synced_at = EXCLUDED.synced_at
            """, tableName, columnNames, placeholders, uniqueKey,
            buildUpdateClause(columns, uniqueKey));
        
        try (var statement = sinkConnection.prepareStatement(insertSql)) {
            sinkConnection.setAutoCommit(false);
            
            for (var row : rows) {
                int paramIndex = 1;
                
                for (String column : rows.get(0).keySet()) {
                    statement.setObject(paramIndex++, row.get(column));
                }
                
                statement.setTimestamp(paramIndex++, Timestamp.from(Instant.now()));
                statement.setString(paramIndex, batchId);
                statement.addBatch();
            }
            
            statement.executeBatch();
            sinkConnection.commit();
            sinkConnection.setAutoCommit(true);
            
        } catch (SQLException e) {
            try {
                sinkConnection.rollback();
                sinkConnection.setAutoCommit(true);
            } catch (SQLException ex) {
                LOG.error("Failed to rollback transaction", ex);
            }
            LOG.errorf("Failed to insert batch for table %s: %s", tableName, e.getMessage());
        }
    }
    
    private String buildUpdateClause(List<String> columns, String uniqueKey) {
        var updates = new ArrayList<String>();
        for (String column : columns) {
            if (!column.equals(uniqueKey) && !column.equals("synced_at") && !column.equals("_replication_batch_id")) {
                updates.add(column + " = EXCLUDED." + column);
            }
        }
        return String.join(", ", updates);
    }
    
    private Timestamp getMaxTimestampFromSource(String tableName, ReplicationConfig.TableConfig tableConfig) {
        String timestampColumn = tableConfig.orderBy().split(",")[0].trim().split(" ")[0];
        String query = String.format("SELECT MAX(%s) as max_ts FROM %s", timestampColumn, tableName);
        
        try (var statement = sourceConnection.createStatement();
             var resultSet = statement.executeQuery(query)) {
            
            if (resultSet.next()) {
                return resultSet.getTimestamp("max_ts");
            }
        } catch (SQLException e) {
            LOG.errorf("Failed to get max timestamp for table %s: %s", tableName, e.getMessage());
        }
        
        return null;
    }
    
    private ProcessedState getLastProcessedValues(String tableName, ReplicationConfig.TableConfig tableConfig) {
        String[] orderColumns = tableConfig.orderBy().split(",");
        String timestampColumn = orderColumns[0].trim().split(" ")[0];
        
        String query;
        if (orderColumns.length > 1) {
            String idColumn = orderColumns[1].trim().split(" ")[0];
            query = String.format("SELECT %s, %s FROM %s ORDER BY %s DESC LIMIT 1",
                                timestampColumn, idColumn, tableName, tableConfig.orderBy());
        } else {
            query = String.format("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1",
                                timestampColumn, tableName, timestampColumn);
        }
        
        try (var statement = sinkConnection.createStatement();
             var resultSet = statement.executeQuery(query)) {
            
            if (resultSet.next()) {
                Timestamp timestamp = resultSet.getTimestamp(timestampColumn);
                String lastId = orderColumns.length > 1 ? resultSet.getString(orderColumns[1].trim().split(" ")[0]) : null;
                return new ProcessedState(timestamp, lastId);
            }
        } catch (SQLException e) {
            LOG.errorf("Failed to get last processed values for table %s: %s", tableName, e.getMessage());
        }
        
        return new ProcessedState(null, null);
    }
    
    private void ensureSinkTable(String tableName) {
        // Implementation for ensuring sink table exists
        // Similar to Python version but using JDBC
        try (var statement = sinkConnection.createStatement()) {
            // This is a simplified version - you'd want to match the source schema
            statement.execute(String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    id BIGINT PRIMARY KEY,
                    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    _replication_batch_id VARCHAR(50)
                )
            """, tableName));
        } catch (SQLException e) {
            LOG.errorf("Failed to ensure sink table %s: %s", tableName, e.getMessage());
        }
    }
    
    public record ReplicationResult(String tableName, long rowsProcessed, long durationMs, boolean success) {}
    private record ProcessedState(Timestamp timestamp, String lastId) {}
}