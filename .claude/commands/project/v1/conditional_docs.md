# Conditional Documentation Guide

This prompt helps you determine what documentation you should read based on the specific changes you need to make in the codebase. Review the conditions below and read the relevant documentation before proceeding with your task.

## Instructions
- Review the task you've been asked to perform
- Check each documentation path in the Conditional Documentation section
- For each path, evaluate if any of the listed conditions apply to your task
  - IMPORTANT: Only read the documentation if any one of the conditions match your task
- IMPORTANT: You don't want to excessively read documentation. Only read the documentation if it's relevant to your task.

## Conditional Documentation

- README.md
  - Conditions:
    - When first understanding the project structure
    - When setting up the development environment
    - When working with environment variables or configuration

- specs/models.md
  - Conditions:
    - When adding or modifying model classes (VersionedModel subclasses)
    - When working with model fields, aliases, or calculated properties
    - When changing model serialization or deserialization behavior

- specs/repositories.md
  - Conditions:
    - When adding or modifying repository implementations
    - When working with database adapters (PostgreSQL, MySQL, MongoDB, SurrealDB, DynamoDB)
    - When changing query, save, or delete behavior in repositories
    - When working with relationships between models

- specs/services.md
  - Conditions:
    - When working with messaging services (RabbitMQ, SQS)
    - When adding or modifying emailing, SMS, or faxing integrations

- specs/sql-migrations.md
  - Conditions:
    - When working with database migrations
    - When adding or modifying migration scripts
    - When changing schema for PostgreSQL or MySQL

- docs/dynamo_db_usage.md
  - Conditions:
    - When working with DynamoDB repositories or adapters
    - When adding DynamoDB-specific features
