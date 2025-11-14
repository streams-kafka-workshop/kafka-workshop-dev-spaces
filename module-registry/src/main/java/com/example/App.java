package com.example;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Main application entry point and menu interface.
 * Demonstrates application structure and orchestrates producer/consumer services.
 */
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    
    public static void main(String[] args) {
        logger.info("Starting Apicurio Registry Producer Demo");
        logger.info("Registry URL: {}", Config.REGISTRY_URL);
        logger.info("Group ID: {}", Config.GROUP_ID);
        logger.info("Artifact ID: {}", Config.ARTIFACT_ID);
        logger.info("Kafka Bootstrap Servers: {}", Config.BOOTSTRAP_SERVERS);

        Scanner scanner = new Scanner(System.in);
        KafkaProducerService producerService = new KafkaProducerService();
        KafkaConsumerService consumerService = new KafkaConsumerService();
        int scenario = 0;
        
        while (scenario != 4) {
            System.out.println("\n======================================");
            System.out.println("Apicurio Registry Producer Demo");
            System.out.println("======================================");
            System.out.println("1. Send custom message (JSON input)");
            System.out.println("2. Fetch and display schema from registry");
            System.out.println("3. Consume messages from topic");
            System.out.println("4. Exit");
            System.out.print("\nSelect scenario (1-4): ");
            
            try {
                scenario = Integer.parseInt(scanner.nextLine());
                
                switch (scenario) {
                    case 1:
                        producerService.sendCustomMessage(scanner);
                        break;
                    case 2:
                        fetchAndDisplaySchema(producerService);
                        break;
                    case 3:
                        consumerService.consumeMessages();
                        break;
                    case 4:
                        System.out.println("Exiting...");
                        break;
                    default:
                        System.out.println("Invalid option. Please select 1-4.");
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a number.");
            } catch (Exception e) {
                logger.error("Error occurred: {}", e.getMessage(), e);
            }
        }
        
        scanner.close();
    }

    private static void fetchAndDisplaySchema(KafkaProducerService producerService) {
        logger.info("\n=== Scenario 2: Fetching LATEST Schema from Registry ===");
        
        try {
            Schema schema = producerService.fetchSchema();
            System.out.println("\n✓ Successfully fetched LATEST schema from registry!");
            System.out.println("\nSchema Definition (LATEST VERSION):");
            System.out.println("==================");
            System.out.println(schema.toString(true));
            
        } catch (Exception e) {
            logger.error("Error fetching schema: {}", e.getMessage(), e);
            System.out.println("✗ Error fetching schema: " + e.getMessage());
        }
    }
}
