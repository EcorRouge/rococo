# Feature Planning

Create a new plan to implement the `Feature` using the exact specified markdown `Plan Format`. Follow the `Instructions` to create the plan use the `Relevant Files` to focus on the right files.

## Variables
issue_number: $1
adw_id: $2
issue_json: $3

## Instructions

- IMPORTANT: You're writing a plan to implement a net new feature based on the `Feature` that will add value to the application.
- IMPORTANT: The `Feature` describes the feature that will be implemented but remember we're not implementing a new feature, we're creating the plan that will be used to implement the feature based on the `Plan Format` below.
- Create the plan in the `specs/` directory with filename: `issue-{issue_number}-adw-{adw_id}-{descriptive-name}.md`
  - Replace `{descriptive-name}` with a short, descriptive name based on the feature (e.g., "add-auth-system", "implement-search", "create-dashboard")
- Use the `Plan Format` below to create the plan.
- Research the codebase to understand existing patterns, architecture, and conventions before planning the feature.
- Detect all version files in the repository using version discovery
- Generate version update instructions (minor bump for features)
- Include version updates in the Version Updates section
- IMPORTANT: Replace every <placeholder> in the `Plan Format` with the requested value. Add as much detail as needed to implement the feature successfully.
- Use your reasoning model: THINK HARD about the feature requirements, design, and implementation approach.
- Follow existing patterns and conventions in the codebase. Don't reinvent the wheel.
- Design for extensibility and maintainability.
- If you need a new library, use `pip install` and add it to `requirements.txt`, and be sure to report it in the `Notes` section of the `Plan Format`.
- IMPORTANT: Add unit tests to validate the feature works correctly and prevent regressions. Unit tests go in `tests/`.
- For all affected components, always increment the version in `setup.py` using a minor bump.
- Respect requested files in the `Relevant Files` section.
- Start your research by reading the `README.md` file.

## Relevant Files

Focus on the following files:

- `README.md` - Contains the project overview and instructions.
- `rococo/models/**` - Data models (VersionedModel, base model classes).
- `rococo/repositories/**` - Repository implementations (PostgreSQL, MySQL, MongoDB, SurrealDB, DynamoDB).
- `rococo/data/**` - Database adapters and connection management.
- `rococo/messaging/**` - Messaging integrations (RabbitMQ, SQS).
- `rococo/migrations/**` - Database migration utilities.
- `rococo/auth/**` - Authentication helpers.
- `rococo/plugins/**` - Plugin system.
- `tests/**` - Test suite.
- `setup.py` - Package version and dependencies.
- `requirements.txt` - Development dependencies.

- Read `.claude/commands/project/v1/conditional_docs.md` to check if your task requires additional documentation
- If your task matches any of the conditions listed, include those documentation files in the `Plan Format: Relevant Files` section of your plan

Ignore all other files in the codebase.

## Plan Format

```md
# Feature: <feature name>

## Metadata
issue_number: `{issue_number}`
adw_id: `{adw_id}`
issue_json: `{issue_json}`

## Feature Description
<describe the feature in detail, including its purpose and value to users>

## User Story
As a <type of user>
I want to <action/goal>
So that <benefit/value>

## Problem Statement
<clearly define the specific problem or opportunity this feature addresses>

## Solution Statement
<describe the proposed solution approach and how it solves the problem>

## Relevant Files
Use these files to implement the feature:

<find and list the files that are relevant to the feature describe why they are relevant in bullet points. If there are new files that need to be created to implement the feature, list them in an h3 'New Files' section.>

## Version Updates
Update the following version files:

<detect the current version in `setup.py` and list the minor-bumped version with specific update instructions>

## Implementation Plan
### Phase 1: Foundation
<describe the foundational work needed before implementing the main feature>

### Phase 2: Core Implementation
<describe the main implementation work for the feature>

### Phase 3: Integration
<describe how the feature will integrate with existing functionality>

## Step by Step Tasks
IMPORTANT: Execute every step in order, top to bottom.

<list step by step tasks as h3 headers plus bullet points. use as many h3 headers as needed to implement the feature. Order matters, start with the foundational shared changes required then move on to the specific implementation. Include creating unit tests throughout the implementation process.>

<Your last step should be running the `Validation Commands` to validate the feature works correctly with zero regressions.>

## Testing Strategy
### Unit Tests
Tests go in `tests/`.
<describe unit tests needed for the feature>

### Edge Cases
<list edge cases that need to be tested>

## Acceptance Criteria
<list specific, measurable criteria that must be met for the feature to be considered complete>

## Validation Commands
Execute every command to validate the feature works correctly with zero regressions.

<list commands you'll use to validate with 100% confidence the feature is implemented correctly with zero regressions. every command must execute without errors so be specific about what you want to run to validate the feature works as expected.>

- `find rococo -name "*.py" -type f -exec python -m py_compile {} +` - Validate Python syntax
- `python -m pytest tests/ -v --tb=short` - Run all unit tests

## Notes
<optionally list any additional notes, future considerations, or context that are relevant to the feature that will be helpful to the developer>
```

## Feature
Extract the feature details from the `issue_json` variable (parse the JSON and use the title and body fields).

## Report

- IMPORTANT: Return exclusively the path to the plan file created and nothing else.