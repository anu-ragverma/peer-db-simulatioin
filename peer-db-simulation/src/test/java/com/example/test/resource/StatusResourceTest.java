// src/test/java/com/example/resource/StatusResourceTest.java
package com.example.resource;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;

import static org.hamcrest.Matchers.*;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StatusResourceTest {
    
    @BeforeEach
    void setUp() {
        // Reset any state before each test
        RestAssured.basePath = "/";
    }
    
    @Test
    @Order(1)
    void testHealthEndpoint() {
        RestAssured.given()
            .when().get("/status/health")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("status", equalTo("healthy"));
    }
    
    @Test
    @Order(2)
    void testGetMigrationStatus() {
        RestAssured.given()
            .when().get("/status/migration")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("totalRecordsMigrated", greaterThanOrEqualTo(0));
    }
    
    // @Test
    // @Order(3)
    // void testGetMigrationSummary() {
    //     RestAssured.given()
    //         .when().get("/status/migration/summary")
    //         .then()
    //         .statusCode(200)
    //         .contentType(ContentType.JSON)
    //         .body("totalRecordsMigrated", greaterThanOrEqualTo(0));
    // }
    
    @Test
    @Order(4)
    void testTableStatusEndpoint() {
        RestAssured.given()
            .when().get("/status/migration/tables/nonexistent")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("tableName", equalTo("nonexistent"))
            .body("recordsMigrated", equalTo(0));
    }
}