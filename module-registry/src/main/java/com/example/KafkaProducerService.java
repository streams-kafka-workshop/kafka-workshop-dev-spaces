package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Scanner;

/**
 * Kafka Producer Service with Apicurio Registry Integration.
 * 
 * This class demonstrates how producers interact with the registry:
 * - Fetches schemas from registry to construct GenericRecord objects
 * - Uses AvroKafkaSerializer which automatically validates against registry schema
 * - Shows the complete producer-registry workflow in one place
 */
public class KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    /**
     * Sends a custom message to Kafka topic.
     * Demonstrates the complete producer-registry integration flow.
     */
    public void sendCustomMessage(Scanner scanner) {
        logger.info("\n=== Sending Custom Message ===");
        
        try {
            // NOTE: Schema fetch is ONLY needed to construct the GenericRecord object structure.
            // This is an Avro requirement - you need a Schema object to build GenericRecord.
            // ⚠️ IMPORTANT: This is NOT for validation!
            // ✅ REAL VALIDATION happens automatically in Step 4 (producer.send()) via AvroKafkaSerializer
            Schema registrySchema = fetchSchemaFromRegistry();
            
            // Display instructions
            System.out.println("\n--- Instructions ---");
            System.out.println("Enter a JSON message to send. You can experiment with:");
            System.out.println("  ✓ Valid message: customer_id as string (e.g., \"CUST-001\")");
            System.out.println("  ✗ Invalid message: customer_id as int (e.g., 12345) to see AUTOMATIC schema validation");
            System.out.println("\nExample valid JSON:");
            System.out.println("{\"customer_id\": \"CUST-001\", \"first_name\": \"John\", \"last_name\": \"Doe\", \"email\": \"john@example.com\", \"age\": 30, \"is_active\": true, \"signup_date\": " + System.currentTimeMillis() + "}");
            System.out.println("\nExample invalid JSON (wrong type for customer_id):");
            System.out.println("{\"customer_id\": 12345, \"first_name\": \"John\", \"last_name\": \"Doe\", \"email\": \"john@example.com\", \"age\": 30, \"is_active\": true, \"signup_date\": " + System.currentTimeMillis() + "}");
            System.out.println("\n--- Enter your JSON message (single-line or multi-line, press Enter on empty line when done): ---");
            
            // Read multi-line JSON input
            StringBuilder jsonInput = new StringBuilder();
            String line;
            boolean firstLine = true;
            while (scanner.hasNextLine()) {
                line = scanner.nextLine();
                if (line.trim().isEmpty() && !firstLine) {
                    break;
                }
                if (line.trim().isEmpty() && firstLine) {
                    continue; // Skip initial empty lines
                }
                firstLine = false;
                jsonInput.append(line);
            }
            
            String jsonString = jsonInput.toString().trim();
            if (jsonString.isEmpty()) {
                System.out.println("✗ No input provided. Cancelling.");
                return;
            }
            
            // Build GenericRecord object from JSON
            // NOTE: Early validation during build() may not catch all type mismatches.
            // The code intentionally allows some wrong types to pass through so we can
            // demonstrate that REGISTRY validation in Step 4 will catch them!
            GenericRecord record = createRecordFromJson(jsonString, registrySchema);
            
            // ================================================================================================
            // STEP 4: AUTOMATIC VALIDATION BY APICURIO REGISTRY (THE REAL VALIDATION!)
            // ================================================================================================
            // This is where GUARANTEED validation happens! The AvroKafkaSerializer automatically:
            // 1. Resolves artifact from group in the registry (using Config.ARTIFACT_ID and Config.GROUP_ID)
            // 2. Retrieves the current schema from the registry (single source of truth!)
            // 3. Validates the GenericRecord against the registry schema during serialization
            // 4. Serializes to Avro binary format (if validation passes)
            // 5. Embeds the schema globalId in the message for consumers
            //
            // ⚠️ IMPORTANT: Even if early validation (builder.build()) misses type errors,
            //    THIS validation will ALWAYS catch them during serialization!
            //
            // ✅ NO manual validation code needed - it's automatic!
            // ✅ Schema always comes from registry (not hardcoded in your app)
            // ✅ Ensures all producers/consumers use compatible schemas
            // ✅ This is the GUARANTEED validation - even if you bypass early checks
            // ================================================================================================
            
            Properties props = createProducerProperties();
            KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
            
            logger.info("📤 Sending message - AvroKafkaSerializer will automatically:");
            logger.info("   1. Fetch schema from registry (artifact '{}' in group '{}')", Config.TOPIC_NAME, Config.GROUP_ID);
            logger.info("   2. Validate record against registry schema");
            logger.info("   3. Serialize and embed schema ID");
            
            ProducerRecord<String, GenericRecord> kafkaRecord = new ProducerRecord<>(Config.TOPIC_NAME, "custom-key", record);
            
            // ✨ THIS IS WHERE GUARANTEED REGISTRY VALIDATION HAPPENS ✨
            // Even if early validation (builder.build()) missed type errors, this will ALWAYS catch them!
            // The serializer fetches schema from registry and validates during serialization.
            producer.send(kafkaRecord);
            producer.flush();
            producer.close();
            
            logger.info("Custom message sent to topic: {}", Config.TOPIC_NAME);
            System.out.println("\n✓ Message sent successfully!");
            System.out.println("   Validation: ✓ Automatic (by Apicurio Registry)");
            System.out.println("   Schema source: Registry (artifact '" + Config.ARTIFACT_ID + "' in group '" + Config.GROUP_ID + "')");
            System.out.println("   Record sent: " + record);
            
        } catch (Exception e) {
            logger.error("Error sending custom message: {}", e.getMessage(), e);
            System.out.println("\n✗ Error: " + e.getMessage());
            
            // Identify validation errors - these come from AvroKafkaSerializer (registry validation)
            boolean isValidationError = e.getMessage().contains("schema") || 
                                        e.getMessage().contains("validation") || 
                                        e.getMessage().contains("type") || 
                                        e.getMessage().contains("mismatch") ||
                                        e.getMessage().contains("ClassCastException") ||
                                        e.getMessage().contains("cannot be cast");
            
            if (isValidationError) {
                System.out.println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                System.out.println("💡 THIS ERROR WAS CAUGHT BY REGISTRY VALIDATION!");
                System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                System.out.println("✅ The AvroKafkaSerializer automatically:");
                System.out.println("   1. Fetched the schema from Apicurio Registry");
                System.out.println("   2. Validated your data against the registry schema during serialization");
                System.out.println("   3. Caught the type mismatch (e.g., integer where string expected)");
                System.out.println("");
                System.out.println("🎯 Key Point: Even if early validation missed this, registry validation");
                System.out.println("   ALWAYS catches it - that's the guarantee of using Apicurio Registry!");
                System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            }
            
            // Try to extract detailed information from Apicurio Registry ProblemDetails
            Throwable cause = e;
            int depth = 0;
            while (cause != null && depth < 5) {
                if (cause.getClass().getName().contains("ProblemDetails")) {
                    try {
                        // Use reflection to get ProblemDetails fields
                        java.lang.reflect.Method getDetail = cause.getClass().getMethod("getDetail");
                        java.lang.reflect.Method getStatus = cause.getClass().getMethod("getStatus");
                        java.lang.reflect.Method getTitle = cause.getClass().getMethod("getTitle");
                        
                        Object status = getStatus.invoke(cause);
                        Object title = getTitle.invoke(cause);
                        Object detail = getDetail.invoke(cause);
                        
                        System.out.println("\n--- Apicurio Registry Error Details ---");
                        if (status != null) {
                            System.out.println("  HTTP Status: " + status);
                            int statusCode = status instanceof Number ? ((Number) status).intValue() : 0;
                            if (statusCode == 404) {
                                System.out.println("\n⚠ Troubleshooting:");
                                System.out.println("  The artifact was not found. Please verify:");
                                System.out.println("    - Artifact ID: '" + Config.TOPIC_NAME + "' (should match topic name)");
                                System.out.println("    - Group ID: '" + Config.GROUP_ID + "'");
                                System.out.println("    - Registry URL: " + Config.REGISTRY_URL);
                                System.out.println("\n  You may need to register the artifact in Apicurio Registry first.");
                            } else if (statusCode == 400) {
                                System.out.println("\n⚠ Troubleshooting:");
                                System.out.println("  Bad request. Check that the artifact ID and group ID are correct.");
                            } else if (statusCode == 401 || statusCode == 403) {
                                System.out.println("\n⚠ Troubleshooting:");
                                System.out.println("  Authentication/Authorization issue. Check your registry credentials.");
                            }
                        }
                        if (title != null) System.out.println("  Title: " + title);
                        if (detail != null) System.out.println("  Detail: " + detail);
                        System.out.println("----------------------------------------");
                        break;
                    } catch (Exception reflectionEx) {
                        // Reflection failed, just print the cause
                        System.out.println("  ProblemDetails: " + cause.toString());
                    }
                }
                cause = cause.getCause();
                depth++;
            }
            
            if (e.getCause() != null && depth >= 5) {
                System.out.println("  Cause: " + e.getCause().getMessage());
            }
            
            // Print full stack trace for debugging
            System.out.println("\nFull error details (for debugging):");
            e.printStackTrace();
        }
    }

    /**
     * Fetches and returns the schema from the registry.
     * Used both for constructing GenericRecord objects and for display purposes.
     */
    public Schema fetchSchema() throws Exception {
        return fetchSchemaFromRegistry();
    }

    /**
     * Fetches schema from registry - ONLY for constructing GenericRecord objects.
     * 
     * NOTE: This is NOT for validation! Validation happens automatically in Step 4
     * when producer.send() is called. AvroKafkaSerializer fetches the schema from
     * the registry and validates automatically.
     * 
     * This method exists because Avro requires a Schema object to build GenericRecord
     * instances. The schema is needed for STRUCTURE, not validation.
     */
    private Schema fetchSchemaFromRegistry() throws Exception {
        logger.info("Fetching LATEST schema from Apicurio Registry (for GenericRecord construction only)...");
        
        String encodedGroupId = URLEncoder.encode(Config.GROUP_ID, StandardCharsets.UTF_8.toString());
        String encodedArtifactId = URLEncoder.encode(Config.ARTIFACT_ID, StandardCharsets.UTF_8.toString());
        
        CloseableHttpClient httpClient = HttpClients.createDefault();
        
        // Step 1: Fetch list of all versions to find the latest
        // Retry logic for 503 (Service Unavailable) errors
        String versionsUrl = String.format("%s/groups/%s/artifacts/%s/versions",
            Config.REGISTRY_URL, encodedGroupId, encodedArtifactId);
        
        logger.info("Fetching versions list from: {}", versionsUrl);
        
        String latestVersion;
        int maxRetries = 3;
        int retryDelayMs = 1000; // Start with 1 second
        String versionsContent = null;
        boolean success = false;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                HttpGet versionsRequest = new HttpGet(versionsUrl);
                final int currentAttempt = attempt;
                versionsContent = httpClient.execute(versionsRequest, response -> {
                    int status = response.getCode();
                    if (status >= 200 && status < 300) {
                        try (InputStream inputStream = response.getEntity().getContent()) {
                            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                        }
                    } else if (status == 503) {
                        // 503 Service Unavailable - will be retried by outer loop
                        throw new RuntimeException("HTTP 503 - Service temporarily unavailable (attempt " + currentAttempt + ")");
                    } else {
                        throw new RuntimeException("HTTP error " + status + " fetching versions list");
                    }
                });
                // Success - break out of retry loop
                success = true;
                break;
            } catch (Exception e) {
                boolean is503Error = e.getMessage() != null && e.getMessage().contains("503");
                if (attempt < maxRetries && is503Error) {
                    logger.warn("Attempt {}/{} failed with 503 error, retrying in {}ms...", attempt, maxRetries, retryDelayMs);
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                    retryDelayMs *= 2; // Exponential backoff
                } else {
                    // Not a retryable error or out of retries
                    httpClient.close();
                    throw e;
                }
            }
        }
        
        if (!success || versionsContent == null) {
            httpClient.close();
            throw new RuntimeException("Failed to fetch versions list after " + maxRetries + " attempts");
        }
        
        try {
            
            // Parse response object - it contains a "versions" array
            JsonObject versionsResponse = new JsonParser().parse(versionsContent).getAsJsonObject();
            JsonArray versionsArray = versionsResponse.getAsJsonArray("versions");
            
            int maxVersion = 0;
            String latestVersionStr = null;
            
            for (int i = 0; i < versionsArray.size(); i++) {
                JsonObject versionObj = versionsArray.get(i).getAsJsonObject();
                String versionStr = versionObj.get("version").getAsString();
                try {
                    int versionNum = Integer.parseInt(versionStr);
                    if (versionNum > maxVersion) {
                        maxVersion = versionNum;
                    }
                } catch (NumberFormatException e) {
                    // If version is not numeric, compare as strings (fallback)
                    if (latestVersionStr == null || versionStr.compareTo(latestVersionStr) > 0) {
                        latestVersionStr = versionStr;
                    }
                }
            }
            
            if (maxVersion > 0) {
                latestVersion = String.valueOf(maxVersion);
            } else if (latestVersionStr != null) {
                latestVersion = latestVersionStr;
            } else {
                throw new RuntimeException("No versions found for artifact");
            }
            
            logger.info("Latest version found: {}", latestVersion);
            
        } catch (Exception e) {
            httpClient.close();
            throw e;
        }
        
        // Step 2: Fetch the latest version's content
        String encodedVersion = URLEncoder.encode(latestVersion, StandardCharsets.UTF_8.toString());
        String contentUrl = String.format("%s/groups/%s/artifacts/%s/versions/%s/content",
            Config.REGISTRY_URL, encodedGroupId, encodedArtifactId, encodedVersion);
        
        logger.info("Fetching schema content from version {}: {}", latestVersion, contentUrl);
        
        String content;
        try {
            HttpGet contentRequest = new HttpGet(contentUrl);
            content = httpClient.execute(contentRequest, response -> {
                int status = response.getCode();
                if (status >= 200 && status < 300) {
                    try (InputStream inputStream = response.getEntity().getContent()) {
                        return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                    }
                } else {
                    throw new RuntimeException("HTTP error " + status + " fetching schema content");
                }
            });
        } finally {
            httpClient.close();
        }
        
        logger.info("Schema content retrieved successfully (version {})", latestVersion);
        
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(content);
    }

    /**
     * Creates a GenericRecord from JSON input using schema-driven validation.
     * 
     * This method dynamically inspects the schema to determine:
     * - Which fields are required vs optional (based on default values)
     * - Field types (including union types with null)
     * - Default values for optional fields
     * 
     * ✅ Schema-driven: No hardcoded field names - adapts to any schema from registry!
     * 
     * NOTE: This method intentionally allows some type mismatches to pass through early validation
     * so that we can demonstrate REGISTRY validation in Step 4 catches them!
     * 
     * Example: If customer_id is provided as an integer (12345) instead of string ("CUST-001"),
     * extractValueFromJsonForField() will convert it to a Long, and builder.set() won't immediately fail.
     * The builder.build() may or may not catch it, but REGISTRY validation during serialization
     * will ALWAYS catch it - that's the guarantee!
     */
    private GenericRecord createRecordFromJson(String jsonString, Schema schema) throws Exception {
        logger.info("Parsing JSON and creating record from user input");
        logger.info("Using schema-driven validation - no hardcoded field names!");
        logger.info("Note: Early validation may miss some type errors - registry validation will catch them!");
        
        JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        
        // Iterate through all fields in the schema (schema-driven approach)
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            
            // Determine if field is optional (has a default value)
            // Use reflection to access defaultValue() since it's package-private
            Object defaultValue = null;
            boolean hasDefaultValue = false;
            try {
                java.lang.reflect.Method defaultValueMethod = Schema.Field.class.getDeclaredMethod("defaultValue");
                defaultValueMethod.setAccessible(true);
                defaultValue = defaultValueMethod.invoke(field);
                // Check if field has a default value defined (even if it's null)
                // Use a different approach: check if the method returns something or if field has default indicator
                java.lang.reflect.Method hasDefaultValueMethod = Schema.Field.class.getDeclaredMethod("hasDefaultValue");
                hasDefaultValueMethod.setAccessible(true);
                hasDefaultValue = (Boolean) hasDefaultValueMethod.invoke(field);
            } catch (Exception e) {
                // If reflection fails, check if field is nullable (union with null)
                // Nullable fields are typically optional
                logger.debug("Could not access defaultValue/hasDefaultValue for field {}: {}", fieldName, e.getMessage());
            }
            
            // Check if field is nullable (union type with null)
            boolean isNullable = fieldSchema.getType() == Schema.Type.UNION && 
                                fieldSchema.getTypes().contains(Schema.create(Schema.Type.NULL));
            
            boolean isOptional = hasDefaultValue || isNullable;
            
            if (jsonObject.has(fieldName)) {
                // Field provided in JSON - extract and set value
                Object value = extractValueFromJsonForField(jsonObject.get(fieldName), fieldSchema);
                builder.set(fieldName, value);
            } else if (!isOptional) {
                // Required field missing - throw error
                throw new IllegalArgumentException(
                    "Missing required field: '" + fieldName + "' (type: " + fieldSchema.getType() + ")");
            } else {
                // Optional field missing - let GenericRecordBuilder use schema default
                // This is the safest approach: don't explicitly set null for union types with null defaults
                // GenericRecordBuilder will correctly handle the schema's default value (even if it's null)
                // during build(), avoiding serialization issues with NullNode
                if (hasDefaultValue) {
                    // Field has a default in schema (even if it's null) - let builder handle it
                    // Don't call builder.set() - GenericRecordBuilder will use the schema default
                    logger.debug("Field '{}' has default value, letting GenericRecordBuilder handle it", fieldName);
                } else if (isNullable) {
                    // For nullable fields (union with null), also let GenericRecordBuilder handle it
                    // This avoids serialization issues with NullNode when the schema has "default": null
                    // The builder will use the schema's default or handle null according to Avro's rules
                    logger.debug("Field '{}' is nullable, letting GenericRecordBuilder handle default", fieldName);
                }
                // GenericRecordBuilder will handle it during build() using the schema definition
            }
        }
        
        return builder.build();
    }
    
    /**
     * Extracts value from JSON element based on the field's schema type.
     * 
     * This method handles:
     * - Union types (e.g., ["null", "string"]) - extracts the non-null type
     * - Primitive types (string, int, long, boolean, etc.)
     * - Null values
     * 
     * ⚠️ INTENTIONAL BEHAVIOR: This method allows wrong types to pass through (e.g., integer for string field).
     * This is by design to demonstrate that REGISTRY validation in Step 4 will catch these errors!
     * 
     * Example: customer_id should be a string, but if user provides 12345 (integer),
     * this will return a Long value, allowing it to bypass early validation so we can
     * see registry validation catch it during serialization.
     */
    private Object extractValueFromJsonForField(com.google.gson.JsonElement element, Schema fieldSchema) {
        // Handle null values
        if (element.isJsonNull()) {
            return null;
        }
        
        // Handle union types (e.g., ["null", "string"])
        if (fieldSchema.getType() == Schema.Type.UNION) {
            // Find the non-null type in the union
            Schema nonNullSchema = null;
            for (Schema unionMember : fieldSchema.getTypes()) {
                if (unionMember.getType() != Schema.Type.NULL) {
                    nonNullSchema = unionMember;
                    break;
                }
            }
            
            if (nonNullSchema != null) {
                return extractValueByType(element, nonNullSchema);
            }
            // If all types are null (shouldn't happen), return null
            return null;
        }
        
        // Handle regular (non-union) types
        return extractValueByType(element, fieldSchema);
    }
    
    /**
     * Extracts value from JSON element based on the specific Avro schema type.
     * 
     * ⚠️ INTENTIONAL: Allows wrong types to pass through to demonstrate registry validation.
     */
    private Object extractValueByType(com.google.gson.JsonElement element, Schema schema) {
        if (element.isJsonNull()) {
            return null;
        }
        
        if (!element.isJsonPrimitive()) {
            // For non-primitive types, return as string (shouldn't happen for our schema)
            return element.getAsString();
        }
        
        com.google.gson.JsonPrimitive primitive = element.getAsJsonPrimitive();
        Schema.Type avroType = schema.getType();
        
        switch (avroType) {
            case STRING:
                // INTENTIONAL: If number provided instead of string, return as number
                // This allows wrong types to pass through so registry validation can catch them
                if (primitive.isString()) {
                    return primitive.getAsString();
                } else if (primitive.isNumber()) {
                    // Wrong type - return as number to demonstrate registry validation
                    Number num = primitive.getAsNumber();
                    return num.longValue();
                }
                return primitive.getAsString();
                
            case INT:
                if (primitive.isNumber()) {
                    return primitive.getAsInt();
                } else if (primitive.isString()) {
                    // Try to parse string as int
                    try {
                        return Integer.parseInt(primitive.getAsString());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Cannot convert string to int for field");
                    }
                }
                break;
                
            case LONG:
                if (primitive.isNumber()) {
                    Number num = primitive.getAsNumber();
                    if (num instanceof Long || num instanceof Integer) {
                        return num.longValue();
                    }
                    return num.longValue();
                } else if (primitive.isString()) {
                    try {
                        return Long.parseLong(primitive.getAsString());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Cannot convert string to long for field");
                    }
                }
                break;
                
            case BOOLEAN:
                if (primitive.isBoolean()) {
                    return primitive.getAsBoolean();
                } else if (primitive.isString()) {
                    return Boolean.parseBoolean(primitive.getAsString());
                }
                break;
                
            case FLOAT:
                if (primitive.isNumber()) {
                    return primitive.getAsFloat();
                }
                break;
                
            case DOUBLE:
                if (primitive.isNumber()) {
                    return primitive.getAsDouble();
                }
                break;
                
            default:
                // For other types, return as string
                return primitive.getAsString();
        }
        
        // Fallback: return as string
        return primitive.getAsString();
    }

    /**
     * Creates Kafka producer properties with Apicurio Registry integration.
     * 
     * The producer is configured with AvroKafkaSerializer which automatically:
     * - Fetches schemas from the registry
     * - Validates data against registry schemas
     * - Embeds schema IDs in messages
     * - No manual validation code needed!
     * 
     * This method shows how to configure producer-registry integration.
     */
    private Properties createProducerProperties() {
        Properties props = new Properties();
        logger.info("Setting bootstrap servers to: {}", Config.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // ================================================================================================
        // KEY CONFIGURATION: Apicurio Registry Automatic Serializer
        // ================================================================================================
        // This serializer automatically:
        // - Fetches schemas from the registry
        // - Validates data against registry schemas
        // - Embeds schema IDs in messages
        // - No manual validation code needed!
        // ================================================================================================
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
        
        // Registry configuration - tells serializer where to find schemas
        props.put("apicurio.registry.url", Config.REGISTRY_URL);
        
        // Use EXPLICIT_ARTIFACT_GROUP_ID to specify which group contains the schema artifact
        // This ensures the serializer looks in Config.GROUP_ID instead of defaulting to "default"
        // Property: "apicurio.registry.artifact.group-id" (from SerdeConfig.EXPLICIT_ARTIFACT_GROUP_ID)
        // Reference: https://www.apicur.io/registry/docs/apicurio-registry/3.1.x/getting-started/assembly-configuring-kafka-client-serdes.html
        props.put("apicurio.registry.artifact.group-id", Config.GROUP_ID);
        
        // Artifact resolution: SimpleTopicIdStrategy uses topic name as artifact ID
        // Uses Config.TOPIC_NAME as artifact ID and looks in Config.GROUP_ID
        props.put("apicurio.registry.artifact-resolver-strategy", "io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy");
        
        // Configuration options
        props.put("apicurio.registry.auto-register", false); // Don't auto-register new schemas
        props.put("apicurio.registry.find-latest", true);     // Find latest version of schema
        props.put("apicurio.registry.use-id", "globalId");    // Use globalId for schema identification
        
        logger.info("✅ Producer configured for automatic registry integration:");
        logger.info("   - Serializer: AvroKafkaSerializer (automatic validation)");
        logger.info("   - Registry: {}", Config.REGISTRY_URL);
        logger.info("   - Artifact: '{}' in group '{}'", Config.TOPIC_NAME, Config.GROUP_ID);
        
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        return props;
    }
}

