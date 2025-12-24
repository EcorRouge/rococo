# Database Integration Tests

Real database integration tests for testing `VersionedModel` and `NonVersionedModel` functionality across MySQL, PostgreSQL, MongoDB, SurrealDB, and DynamoDB.

## Overview

These tests verify that:

1. **VersionedModel** (with Big 6 fields) works correctly:
   - Entity creation with automatic field population (entity_id, version, previous_version, changed_on, changed_by_id, active)
   - Version bumping on updates
   - Soft delete (active=False)
   - Audit table records

2. **NonVersionedModel** works correctly:
   - Simple entity creation without versioning overhead
   - Updates without version tracking
   - No audit table operations
   - Hard delete

## Prerequisites

- Python 3.10+
- pytest (`pip install pytest`)
- Database-specific drivers (installed via rococo dependencies)
- Docker (for running database containers)
- Environment variables configured for database connections

## Required Environment Variables

The tests require environment variables to connect to databases. Tests will be **automatically skipped** if the required variables are not set.

### MySQL

```bash
export MYSQL_HOST=localhost
export MYSQL_PORT=4000
export MYSQL_USER=database
export MYSQL_PASSWORD=database
export MYSQL_DATABASE=database
```

### PostgreSQL

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=4001
export POSTGRES_USER=database
export POSTGRES_PASSWORD=database
export POSTGRES_DATABASE=database
```

### MongoDB

```bash
export MONGODB_HOST=localhost
export MONGODB_PORT=4003
export MONGODB_DATABASE=database
```

### SurrealDB

```bash
export SURREALDB_HOST=localhost
export SURREALDB_PORT=4002
export SURREALDB_USER=database
export SURREALDB_PASSWORD=database
export SURREALDB_NAMESPACE=test
export SURREALDB_DATABASE=database
```

### DynamoDB

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export DYNAMODB_ENDPOINT_URL=http://localhost:8000  # Optional, for local DynamoDB
```

## Quick Start

### Step 1: Start Database Containers

Run these Docker commands to start all database containers:

```bash
# MySQL on port 4000
docker run -d \
  --name mysql-integration-test \
  -p 4000:3306 \
  -e MYSQL_ROOT_PASSWORD=database \
  -e MYSQL_DATABASE=database \
  -e MYSQL_USER=database \
  -e MYSQL_PASSWORD=database \
  mysql:latest

# PostgreSQL on port 4001
docker run -d \
  --name postgres-integration-test \
  -p 4001:5432 \
  -e POSTGRES_USER=database \
  -e POSTGRES_PASSWORD=database \
  -e POSTGRES_DB=database \
  postgres:latest

# SurrealDB on port 4002
docker run -d \
  --name surrealdb-integration-test \
  -p 4002:8000 \
  surrealdb/surrealdb:latest start --user database --pass database

# MongoDB on port 4003
docker run -d \
  --name mongodb-integration-test \
  -p 4003:27017 \
  mongo:latest

# DynamoDB Local on port 8000
docker run -d \
  --name dynamodb-integration-test \
  -p 8000:8000 \
  amazon/dynamodb-local
```

### Step 2: Set Environment Variables

Create a `.env` file or export variables:

```bash
# MySQL
export MYSQL_HOST=localhost
export MYSQL_PORT=4000
export MYSQL_USER=database
export MYSQL_PASSWORD=database
export MYSQL_DATABASE=database

# PostgreSQL
export POSTGRES_HOST=localhost
export POSTGRES_PORT=4001
export POSTGRES_USER=database
export POSTGRES_PASSWORD=database
export POSTGRES_DATABASE=database

# MongoDB
export MONGODB_HOST=localhost
export MONGODB_PORT=4003
export MONGODB_USER=database
export MONGODB_PASSWORD=database
export MONGODB_DATABASE=database

# SurrealDB
export SURREALDB_HOST=localhost
export SURREALDB_PORT=4002
export SURREALDB_USER=database
export SURREALDB_PASSWORD=database
export SURREALDB_NAMESPACE=test
export SURREALDB_DATABASE=database

# DynamoDB
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export DYNAMODB_ENDPOINT_URL=http://localhost:8000
```

Or source them from a file:

```bash
source .env
```

### Step 3: Run Tests

```bash
cd /path/to/rococo
pytest tests/database-integration/ -v
```

## Running Tests

### Run All Integration Tests

```bash
pytest tests/database-integration/ -v
```

### Run Tests for Specific Database

```bash
# MySQL tests only
pytest tests/database-integration/test_mysql_integration.py -v

# PostgreSQL tests only
pytest tests/database-integration/test_postgres_integration.py -v

# MongoDB tests only
pytest tests/database-integration/test_mongodb_integration.py -v

# SurrealDB tests only
pytest tests/database-integration/test_surrealdb_integration.py -v

# DynamoDB tests only
pytest tests/database-integration/test_dynamodb_integration.py -v
```

### Run Specific Test Class

```bash
# Run only VersionedModel tests for MongoDB
pytest tests/database-integration/test_mongodb_integration.py::TestMongoDBVersionedModel -v

# Run only NonVersionedModel tests for MySQL
pytest tests/database-integration/test_mysql_integration.py::TestMySQLNonVersionedModel -v
```

### Run Specific Test

```bash
# Run a single test
pytest tests/database-integration/test_postgres_integration.py::TestPostgreSQLVersionedModel::test_create_versioned_product -v
```

### Skip Tests

Tests will automatically skip if environment variables are not configured:

```bash
# This will skip all database tests if no env vars are set
pytest tests/database-integration/ -v

# Output:
# test_mongodb_integration.py::TestMongoDBVersionedModel::test_create SKIPPED
# Reason: MongoDB configuration not available. Set MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE environment variables.
```

## Test Structure

Each database has comprehensive test coverage:

- **VersionedModel Tests** (~20-30 tests per database):
  - Create operations with Big 6 field validation
  - Update operations with version bumping
  - Soft delete with audit table verification
  - Repository pattern tests
  - Message queue integration tests

- **NonVersionedModel Tests** (~30-40 tests per database):
  - Create operations without versioning
  - Update operations without version tracking
  - Hard delete (no audit tables)
  - Extra fields support
  - Pagination and filtering
  - Edge cases

- **Integration Tests**:
  - Cross-model operations
  - Real-world scenarios
  - Performance characteristics

## Known Failures

Some tests are marked with `@pytest.mark.known_failure` to document known bugs. See `KNOWN_FAILURES.md` for details.

## Cleaning Up

Stop and remove all database containers:

```bash
docker stop mysql-integration-test postgres-integration-test surrealdb-integration-test mongodb-integration-test dynamodb-integration-test
docker rm mysql-integration-test postgres-integration-test surrealdb-integration-test mongodb-integration-test dynamodb-integration-test
```

## Troubleshooting

### Tests are being skipped

Ensure environment variables are set correctly:

```bash
# Check if variables are set
env | grep -E 'MYSQL|POSTGRES|MONGODB|SURREALDB|AWS'
```

### Connection refused errors

Ensure Docker containers are running:

```bash
docker ps | grep -E 'mysql|postgres|mongo|surrealdb|dynamodb'
```

### Port already in use

If ports 4000-4003 or 8000 are already in use, change the port mappings in Docker commands and update environment variables accordingly.

## CI/CD Integration

For CI/CD pipelines, you can:

1. Use GitHub Actions services/containers
2. Use Docker Compose for local testing
3. Skip integration tests in CI if databases are not available:

```bash
# Run only unit tests (skip integration tests)
pytest tests/ -v --ignore=tests/database-integration/
```

## Contributing

When adding new database adapters:

1. Create a new test file: `test_<database>_integration.py`
2. Add environment variable configuration to `conftest.py`
3. Add skip decorator using the config function
4. Update this README with environment variables and Docker commands
5. Follow the existing test structure for consistency



## Variables List

```bash
export MYSQL_HOST=localhost
export MYSQL_PORT=4000
export MYSQL_USER=database
export MYSQL_PASSWORD=database
export MYSQL_DATABASE=database
export POSTGRES_HOST=localhost
export POSTGRES_PORT=4001
export POSTGRES_USER=database
export POSTGRES_PASSWORD=database
export POSTGRES_DATABASE=database
export MONGODB_HOST=localhost
export MONGODB_PORT=4003
export MONGODB_DATABASE=database
export SURREALDB_HOST=localhost
export SURREALDB_PORT=4002
export SURREALDB_USER=database
export SURREALDB_PASSWORD=database
export SURREALDB_NAMESPACE=database
export SURREALDB_DATABASE=database
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export DYNAMODB_ENDPOINT_URL=http://localhost:8000
```
