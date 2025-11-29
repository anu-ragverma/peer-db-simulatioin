// src/test/java/com/example/integration/BasicIntegrationTest.java
package com.example.integration;

import com.example.service.ReplicationOrchestrator;
import com.example.service.StateService;
import com.example.service.StatusService;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class BasicIntegrationTest {
    
    @Inject
    ReplicationOrchestrator orchestrator;
    
    @Inject
    StateService stateService;
    
    @Inject
    StatusService statusService;
    
    @Test
    void testAllServicesAreInjected() {
        assertNotNull(orchestrator, "ReplicationOrchestrator should be injected");
        assertNotNull(stateService, "StateService should be injected");
        assertNotNull(statusService, "StatusService should be injected");
    }
    
    @Test
    void testStateServiceInitialization() {
        assertDoesNotThrow(() -> stateService.initializeStateTable(), 
            "State table initialization should not throw exceptions");
    }
    
    @Test
    void testStatusServiceInitialization() {
        assertDoesNotThrow(() -> statusService.initializeMigrationTable(), 
            "Migration table initialization should not throw exceptions");
    }
    
    @Test
    void testPerformanceEstimation() {
        var estimate = orchestrator.estimatePerformance();
        assertNotNull(estimate, "Performance estimate should not be null");
        assertTrue(estimate.totalVirtualThreads() >= 0, "Virtual threads count should be non-negative");
        assertTrue(estimate.estimatedRowsPerSecond() >= 0, "Estimated rows per second should be non-negative");
    }
    
    @Test
    void testStatusTracking() {
        statusService.startReplication();
        
        statusService.updateBatchStatus("test_table", "batch1", 100, 
            java.time.Instant.now().minusSeconds(60),
            java.time.Instant.now(),
            "COMPLETED", null);
        
        var status = statusService.getCurrentStatus();
        assertNotNull(status, "Status should not be null");
        assertEquals(100L, status.totalRecordsMigrated(), "Should track 100 migrated records");
        assertNotNull(status.startTime(), "Replication start time should be set");
    }
}