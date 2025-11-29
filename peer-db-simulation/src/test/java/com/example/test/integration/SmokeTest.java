// src/test/java/com/example/integration/SmokeTest.java
package com.example.integration;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class SmokeTest {
    
    @Test
    void testBasicFunctionality() {
        // Simple test to verify the application starts
        assertTrue(true, "Basic test should pass");
    }
    
    @Test
    void testEnvironment() {
        String env = System.getProperty("quarkus.datasource.db-kind", "unknown");
        assertTrue(env.equals("h2") || env.equals("unknown"), 
            "Datasource should be H2 or default");
    }
}