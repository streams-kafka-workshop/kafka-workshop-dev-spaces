#!/bin/bash

# Apicurio Registry Producer Runner

echo "Apicurio Registry Producer Demo"
echo "================================"
echo ""

# Build if not already built
if [ ! -f "target/apicurio-registry-producer-1.0.0.jar" ]; then
    echo "Building the project..."
    mvn clean package -DskipTests
    echo ""
fi

# Run the application
echo "Starting the application..."
echo ""
java -jar target/apicurio-registry-producer-1.0.0.jar
