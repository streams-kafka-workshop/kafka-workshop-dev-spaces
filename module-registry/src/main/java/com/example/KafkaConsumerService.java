package com.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka Consumer Service with Apicurio Registry Integration.
 * 
 * This class demonstrates how consumers interact with the registry:
 * - Uses AvroKafkaDeserializer which automatically fetches schemas from registry
 * - Shows schema resolution happening transparently during deserialization
 * - Demonstrates consumer-registry workflow in one place
 */
public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    /**
     * Consumes messages from Kafka topic.
     * The AvroKafkaDeserializer automatically:
     * - Extracts schema ID from message
     * - Fetches schema from registry
     * - Deserializes using the correct schema version
     */
    public void consumeMessages() {
        logger.info("\n=== Scenario 4: Consuming Messages ===");
        
        try {
            Properties props = createConsumerProperties();
            KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
            
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(Config.TOPIC_NAME));
            
            System.out.println("✓ Consumer started. Waiting for messages...");
            System.out.println("Press Ctrl+C to stop consuming");
            
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    logger.info("Received message - Key: {}, Partition: {}, Offset: {}", 
                        record.key(), record.partition(), record.offset());
                    
                    try {
                        // Automatic deserialization with Apicurio Registry
                        // The AvroKafkaDeserializer automatically:
                        // 1. Extracts schema globalId from the message
                        // 2. Fetches the schema from the registry
                        // 3. Deserializes the message using the correct schema
                        GenericRecord deserializedRecord = record.value();
                        
                        System.out.println("✓ Message received and deserialized:");
                        System.out.println("  Key: " + record.key());
                        System.out.println("  Value: " + deserializedRecord);
                        System.out.println("  Partition: " + record.partition());
                        System.out.println("  Offset: " + record.offset());
                        System.out.println();
                        
                    } catch (Exception e) {
                        logger.error("Error deserializing message: {}", e.getMessage());
                        System.out.println("✗ Error deserializing message: " + e.getMessage());
                    }
                }
            }
            
        } catch (Exception e) {
            logger.error("Error consuming messages: {}", e.getMessage(), e);
            System.out.println("✗ Error: " + e.getMessage());
        }
    }

    /**
     * Creates Kafka consumer properties with Apicurio Registry integration.
     * 
     * The consumer uses AvroKafkaDeserializer which automatically:
     * - Resolves schemas from the registry using schema IDs embedded in messages
     * - Deserializes messages using the correct schema version
     * - Ensures schema compatibility
     * 
     * This method shows how to configure consumer-registry integration.
     */
    private Properties createConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // Using Apicurio Registry automatic deserializer (requires Java 17+)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
        // Configure Apicurio Registry URL
        props.put("apicurio.registry.url", Config.REGISTRY_URL);
        // Use EXPLICIT_ARTIFACT_GROUP_ID to explicitly specify which group to use for artifact resolution
        // Property name from SerdeConfig.EXPLICIT_ARTIFACT_GROUP_ID -> "apicurio.registry.artifact.group-id"
        // Reference: https://www.apicur.io/registry/docs/apicurio-registry/3.1.x/getting-started/assembly-configuring-kafka-client-serdes.html
        props.put("apicurio.registry.artifact.group-id", Config.GROUP_ID);
        // Configure artifact resolver
        props.put("apicurio.registry.artifact-resolver-strategy", "io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy");
        props.put("apicurio.registry.auto-register", false);
        props.put("apicurio.registry.find-latest", true);
        props.put("apicurio.registry.use-id", "globalId");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}

