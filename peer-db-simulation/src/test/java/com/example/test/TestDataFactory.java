// src/test/java/com/example/test/TestDataFactory.java
package com.example.test;

import com.example.service.StatusService;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestDataFactory {
    
    public static StatusService.MigrationStatus createMockMigrationStatus(
        long totalRecords, boolean isRunning, int tableCount) {
        
        Instant startTime = Instant.now().minusSeconds(3600);
        Instant endTime = isRunning ? null : Instant.now();
        
        List<StatusService.TableStatus> tableStatuses = List.of(
            new StatusService.TableStatus("users", totalRecords / 2, 25, 1, Instant.now()),
            new StatusService.TableStatus("orders", totalRecords / 2, 30, 0, Instant.now())
        );
        
        return new StatusService.MigrationStatus(
            startTime,
            endTime,
            totalRecords,
            tableStatuses,
            -1.0,
            new StatusService.ThroughputMetrics(1000L, 60000L, 3600000L)
        );
    }
    
    public static List<Map<String, Object>> createTestUserData(int count) {
        return IntStream.range(1, count + 1)
            .mapToObj(i -> Map.<String, Object>of(
                "id", (long) i,
                "name", "User " + i,
                "email", "user" + i + "@example.com",
                "created_at", java.sql.Timestamp.from(Instant.now())
            ))
            .collect(Collectors.toList());
    }
}