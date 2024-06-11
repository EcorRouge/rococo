# Use `_audit` tables for auditing purposes

- Status: accepted
- Date: 2023-11-18

## Context and Problem Statement

For audit purposes, every change to an entity in the database creates a new version of the entity. We store all versions of the entity in the same table/collection. The `latest` attribute (boolean) is used to distinguish between the current version of an entity and it's previous version.

A couple of issues with this approach is:  
- It does not let us use record links in SurrealDB
- We faced locking issues while trying to clean out large table in RansomSnare MySQL database.

## Decision Outcome

Instead of storing `latest`, we’ll have an `_audit` table for each data table.  Instead of marking records with the same `entity_id` as `latest=false`, we’ll `SELECT INTO` the `_audit` table from the data table and then upsert the new data.  

We expect this approach to give us all of the benefits of Big6 while allowing us to use the native features of SurrealDB effectively.