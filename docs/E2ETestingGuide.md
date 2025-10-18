# Fluss E2E Testing Guide

This guide provides instructions and best practices for developing end-to-end (E2E) tests for the Fluss project.

## Table of Contents
1. [Introduction](#introduction)
2. [Test Structure](#test-structure)
3. [Using FlussClusterExtension](#using-flussclusterextension)
4. [Class Loading Tests](#class-loading-tests)
5. [Writing Effective E2E Tests](#writing-effective-e2e-tests)
6. [Running E2E Tests](#running-e2e-tests)

## Introduction

Fluss E2E tests verify the complete functionality of the system by testing interactions between different components in a realistic environment. These tests use the `FlussClusterExtension` to start a complete Fluss cluster including CoordinatorServer, TabletServers, and ZooKeeper.

## Test Structure

E2E tests should follow the standard JUnit 5 structure:

```java
@RegisterExtension
static final FlussClusterExtension FLUSS_CLUSTER =
        FlussClusterExtension.builder()
                .setNumOfTabletServers(3)
                .build();

@Test
void testMyFeature() {
    // Test implementation
}
```

## Using FlussClusterExtension

The `FlussClusterExtension` provides a complete Fluss cluster for testing:

```java
// Get client configuration
Configuration clientConfig = FLUSS_CLUSTER.getClientConfig();

// Get RPC client
RpcClient rpcClient = FLUSS_CLUSTER.getRpcClient();

// Get coordinator gateway
CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER.newCoordinatorClient();

// Get tablet server gateway
TabletServerGateway tabletServerGateway = FLUSS_CLUSTER.newTabletServerClientForNode(0);
```

## Class Loading Tests

Fluss heavily relies on plugin architecture, making class loading isolation critical. Use the `ClassLoaderTestUtils` for testing class loading scenarios:

```java
// Create isolated class loader
URLClassLoader isolatedClassLoader = ClassLoaderTestUtils.createIsolatedClassLoader(
        "/path/to/plugin.jar");

// Load class with isolation
Class<?> pluginClass = ClassLoaderTestUtils.loadPluginWithIsolation(
        "com.example.PluginClass", "/path/to/plugin.jar");

// Verify isolation
boolean isIsolated = ClassLoaderTestUtils.verifyClassLoaderIsolation(
        class1, class2);
```

## Writing Effective E2E Tests

### Best Practices

1. **Use descriptive test names**: Test names should clearly describe what is being tested
2. **Keep tests focused**: Each test should verify one specific behavior
3. **Clean up resources**: Ensure tests properly close connections and clean up data
4. **Use appropriate timeouts**: Use `CommonTestUtils.retry()` for operations that may take time

### Example Test

```java
@Test
void testTableCreationAndDataIngestion() throws Exception {
    // Arrange
    CoordinatorGateway coordinator = FLUSS_CLUSTER.newCoordinatorClient();
    Configuration clientConfig = FLUSS_CLUSTER.getClientConfig();
    
    // Act
    // Create table using coordinator
    CreateTableRequest request = new CreateTableRequest(...);
    coordinator.createTable(request).get();
    
    // Connect client and write data
    try (Connection connection = ConnectionFactory.createConnection(clientConfig)) {
        // Write data
    }
    
    // Assert
    // Verify data was written correctly
}
```

## Running E2E Tests

### Maven Commands

```bash
# Run all E2E tests
mvn verify -Ptest-e2e

# Run with coverage
mvn verify -Ptest-e2e -Ptest-coverage

# Run specific test class
mvn test -Ptest-e2e -Dtest=MyE2ETest
```

### Test Profiles

- `test-e2e`: Runs E2E tests with 90% coverage requirement
- `test-coverage`: Enables coverage collection
- `test-core`: Runs core unit tests

## Conclusion

E2E tests are crucial for ensuring Fluss works correctly in realistic scenarios. Follow these guidelines to write effective, maintainable tests that verify the complete system functionality.