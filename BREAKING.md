# Breaking Changes and Migrations Guide


## 1.0.11

The Pydantic package version changed from v1 to v2 due to the SurrealDb dependency update. SurrealDb 0.3.2 requires Pydantic >=2.1.0.
If you use Rococo v 1.0.11 in a project that uses Pydantic v1, you will hit this breaking change.  
  
The Migration Guide for transition from Pydantic v1 to Pydantic v2 describes what needs to be changed. And there is a code transformation tool that should help with the migration. Most likely only a small portion of that guide will be relevant.  
  
PS. Note that Rococo version did not communicate the breaking change according to SemVer. We plan to follow SemVer as a part of our overall versioning strategy.

## 1.0.36

Rococo v1.0.36 introduces optional dependency groups (extras) to help avoid installing unused libraries by default.

What's changed
The following extras are now available:

* rococo[data] - for database-related functionality: adapters, repositories, migrations.
* rococo[emailing] - for email features
* rococo[messaging] - for messaging components
* rococo[all] - includes everything

Action required
If your project uses:

* rococo.repositories or rococo.data, update your dependencies to use rococo[data] or rococo[all]
* rococo.emailing - use rococo[emailing]
* rococo.messaging - use rococo[messaging]

Update your requirements.txt or pyproject.toml to reflect these changes.

## 1.3.4

`OpenObserve.enable_postgres_tracing()` no longer enables SQL commenting by default.

What's changed
In 1.3.3 the method always instrumented psycopg2 with `enable_commenter=True`, which appends trace context as a comment to every statement. That rewrites each statement, which hurts prepared-statement/plan reuse and fragments `pg_stat_statements` entries. It is now off by default and opt-in per call:

```python
provider.enable_postgres_tracing()                        # no SQL commenting (new default)
provider.enable_postgres_tracing(enable_commenter=True)   # previous 1.3.3 behaviour
```

Action required
Spans are still produced for every query either way, so no change is needed unless you rely on trace ids appearing in Postgres logs or `pg_stat_statements`. If you do, pass `enable_commenter=True`.
