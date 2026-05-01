# Chore Planning

Create a new plan to resolve the `Chore` using the exact specified markdown `Plan Format`. Follow the `Instructions` to create the plan use the `Relevant Files` to focus on the right files. Follow the `Report` section to properly report the results of your work.

## Variables
issue_number: $1
adw_id: $2
issue_json: $3

## Instructions

- IMPORTANT: You're writing a plan to resolve a chore based on the `Chore` that will add value to the application.
- IMPORTANT: The `Chore` describes the chore that will be resolved but remember we're not resolving the chore, we're creating the plan that will be used to resolve the chore based on the `Plan Format` below.
- You're writing a plan to resolve a chore, it should be simple but we need to be thorough and precise so we don't miss anything or waste time with any second round of changes.
- Create the plan in the `specs/` directory with filename: `issue-{issue_number}-adw-{adw_id}-{descriptive-name}.md`
  - Replace `{descriptive-name}` with a short, descriptive name based on the chore (e.g., "update-readme", "fix-tests", "refactor-auth")
- Use the plan format below to create the plan.
- Research the codebase and put together a plan to accomplish the chore.
- Detect all version files in the repository using version discovery
- Generate version update instructions (patch bump for chores)
- Include version updates in the Version Updates section
- IMPORTANT: Replace every <placeholder> in the `Plan Format` with the requested value. Add as much detail as needed to accomplish the chore.
- Use your reasoning model: THINK HARD about the plan and the steps to accomplish the chore.
- For all affected components, always increment the version in `setup.py` using a patch bump.
- Respect requested files in the `Relevant Files` section.
- Start your research by reading the `README.md` file.
- When you finish creating the plan for the chore, follow the `Report` section to properly report the results of your work.

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
# Chore: <chore name>

## Metadata
issue_number: `{issue_number}`
adw_id: `{adw_id}`
issue_json: `{issue_json}`

## Chore Description
<describe the chore in detail>

## Relevant Files
Use these files to resolve the chore:

<find and list the files that are relevant to the chore describe why they are relevant in bullet points. If there are new files that need to be created to accomplish the chore, list them in an h3 'New Files' section.>

## Version Updates
Update the following version files:

<detect the current version in `setup.py` and list the patch-bumped version with specific update instructions>

## Step by Step Tasks
IMPORTANT: Execute every step in order, top to bottom.

<list step by step tasks as h3 headers plus bullet points. use as many h3 headers as needed to accomplish the chore. Order matters, start with the foundational shared changes required to fix the chore then move on to the specific changes required to fix the chore. Your last step should be running the `Validation Commands` to validate the chore is complete with zero regressions.>

## Validation Commands
Execute every command to validate the chore is complete with zero regressions.

<list commands you'll use to validate with 100% confidence the chore is complete with zero regressions. every command must execute without errors so be specific about what you want to run to validate the chore is complete with zero regressions. Don't validate with curl commands.>
- `find rococo -name "*.py" -type f -exec python -m py_compile {} +` - Validate Python syntax
- `python -m pytest tests/ -v --tb=short` - Run all unit tests

## Notes
<optionally list any additional notes or context that are relevant to the chore that will be helpful to the developer>
```

## Chore
Extract the chore details from the `issue_json` variable (parse the JSON and use the title and body fields).

## Report

- IMPORTANT: Return exclusively the path to the plan file created and nothing else.