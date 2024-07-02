# Breaking Changes and Migrations Guide


## 1.0.11

The Pydantic package version changed from v1 to v2 due to the SurrealDb dependency update. SurrealDb 0.3.2 requires Pydantic >=2.1.0.
If you use Rococo v 1.0.11 in a project that uses Pydantic v1, you will hit this breaking change.  
  
The Migration Guide for transition from Pydantic v1 to Pydantic v2 describes what needs to be changed. And there is a code transformation tool that should help with the migration. Most likely only a small portion of that guide will be relevant.  
  
PS. Note that Rococo version did not communicate the breaking change according to SemVer. We plan to follow SemVer as a part of our overall versioning strategy.