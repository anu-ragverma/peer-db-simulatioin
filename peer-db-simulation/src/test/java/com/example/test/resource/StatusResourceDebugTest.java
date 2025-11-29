// src/test/java/com/example/resource/StatusResourceDebugTest.java
package com.example.resource;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import io.restassured.RestAssured;

import static org.hamcrest.Matchers.*;

@QuarkusTest
class StatusResourceDebugTest {
    
    @Test
    void testSimpleHealth() {
        RestAssured.given()
            .when().get("/status/health")
            .then()
            .statusCode(200)
            .body("status", equalTo("healthy"));
    }
    
    @Test
    void testMigrationEndpoint() {
        RestAssured.given()
            .when().get("/status/migration")
            .then()
            .statusCode(200);
    }
    
    // @Test
    // void testMigrationSummary() {
    //     RestAssured.given()
    //         .when().get("/status/migration/summary")
    //         .then()
    //         .statusCode(200)
    //         .body("totalRecordsMigrated", greaterThanOrEqualTo(0));
    // }
}