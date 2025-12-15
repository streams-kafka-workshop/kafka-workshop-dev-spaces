package com.example;

/**
 * Configuration constants shared across producer and consumer services.
 * Contains registry and Kafka connection settings.
 */
public class Config {
    // Registry configuration
    public static final String REGISTRY_URL = 
        "<REPLACE_WITH_REGISTRY_URL>";
    
    // Match artifact ID to topic name for SimpleTopicIdStrategy compatibility
    public static final String GROUP_ID = "my-group";  // Group where artifact is registered
    public static final String ARTIFACT_ID = "schema-demo";  // Match topic name
    public static final String ARTIFACT_VERSION = "1";
    public static final String TOPIC_NAME = "schema-demo";
    public static final String BOOTSTRAP_SERVERS = "<REPLACE_WITH_BOOTSTRAP_SERVER_ADDRESS>";
}

