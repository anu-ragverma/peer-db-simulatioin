// src/main/java/com/example/qualifier/SinkDatabase.java
package com.example.qualifier;

import jakarta.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Qualifier
@Retention(RetentionPolicy.RUNTIME)
public @interface SinkDatabase {
}