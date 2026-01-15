# Release
> Create a new GitHub release using the current tag and auto-generated release notes

## Instructions

1. Verify that HEAD has a tag (current tag)
2. Generate release notes from commits between current and previous tags
3. Create a concise release title from the commits
4. Create a GitHub release with the generated notes
5. Wait for GitHub Actions to complete successfully
6. Display the release URL and summary

## Steps

### 1. Verify Current Tag
- Get the tag at HEAD: `git describe --exact-match HEAD`
- If HEAD doesn't have a tag, fail with error message: "HEAD must be tagged before creating a release"
- Store the current tag (e.g., `v1.2.1`)

### 2. Generate Release Notes
- Get the previous tag: `git tag --sort=-version:refname | head -n 2 | tail -n 1`
- If less than 2 tags exist, fail with message: "Need at least 2 tags to generate release notes"
- Get commit messages: `git log <previous-tag>..<current-tag> --pretty=format:"%s" --no-merges`
- If no commits found, fail with message: "No commits found between tags"

### 3. Categorize Commits
Parse each commit message and categorize based on conventional commit prefixes:

**New Features:**
- Commits starting with: `feat:`, `feature:`, `add:`, `new:`
- Commits containing: "add", "implement", "introduce" (case insensitive)

**Bug Fixes:**
- Commits starting with: `fix:`, `bugfix:`, `hotfix:`
- Commits containing: "fix", "bug", "resolve", "correct" (case insensitive)

**Improvements:**
- Commits starting with: `refactor:`, `perf:`, `improve:`, `chore:`, `update:`, `enhance:`
- Commits containing: "improve", "refactor", "optimize", "update", "enhance", "upgrade" (case insensitive)
- Commits starting with: `docs:`, `doc:`, `style:`, `test:`

**Uncategorized:**
- Any commits that don't fit the above categories go to "Other Changes"

### 4. Create Release Title
Generate a concise title in the format: `{tag} - {brief summary}`

**Title Generation Rules:**
- Analyze all commits to create a brief summary (max 80 characters)
- If single commit: Use cleaned commit message as summary
- If multiple commits in same category: Use category-based summary
  - New Features: "New features and enhancements"
  - Bug Fixes: "Bug fixes and corrections"
  - Improvements: "Improvements and updates"
- If mixed categories: Combine top 2 categories (e.g., "Bug fixes and improvements")
- Remove conventional commit prefixes from the summary
- Capitalize first letter
- Examples:
  - `v1.2.1 - Fix active flag for PostgreSQL and SurrealDB`
  - `v1.2.0 - New features and bug fixes`
  - `v1.1.9 - Performance improvements and dependency updates`

### 5. Format Release Notes Body
Generate the release notes body in markdown format:

```markdown
## New Features
- Feature description 1
- Feature description 2

## Bug Fixes
- Bug fix description 1
- Bug fix description 2

## Improvements
- Improvement description 1
- Improvement description 2

## Other Changes
- Other change 1

---
**Full Changelog**: {previous-tag}...{current-tag}
```

**Formatting Rules:**
- Remove conventional commit prefixes (feat:, fix:, etc.)
- Capitalize first letter of each bullet point
- Remove issue numbers and PR references from the end (e.g., (#123), GH-456)
- Omit empty categories (don't show sections with 0 commits)
- Add a "Full Changelog" link at the bottom comparing the two tags

### 6. Create GitHub Release
- Create the release: `gh release create <tag> --title "<release-title>" --notes "<release-notes>" --latest`
- The `--latest` flag marks this as the latest release
- If `gh` CLI is not installed, fail with installation instructions
- If release creation fails, fail with error message

### 7. Monitor GitHub Actions
- Wait a few seconds for GitHub Actions to start (use `sleep 5` or similar)
- List workflow runs for the tag: `gh run list --json status,conclusion,databaseId --limit 5`
- Find the most recent workflow run that was triggered by the release
- Get the workflow run ID
- Use `gh run watch <run-id>` to monitor the workflow in real-time
- Wait for workflow to complete (success, failure, or cancelled)
- Timeout after 10 minutes if workflow doesn't complete
- If workflow conclusion is not `success`, display warning but do not fail: "Warning: GitHub Actions workflow did not complete successfully"
- Display workflow status (success, failure, etc.)
- Display workflow URL: `gh run view <run-id> --json url --jq .url`

### 8. Display Release Information
- Show the release title
- Show the release tag
- Display the release URL: `gh release view <tag> --json url --jq .url` (just show URL, don't open)
- Display GitHub Actions workflow status
- Display GitHub Actions workflow URL
- Confirm release was created successfully

## Error Handling

- If git repository is not found, fail with error message
- If HEAD is not tagged, fail with error: "HEAD must be tagged before creating a release. Create a tag first with: git tag v{version}"
- If less than 2 tags exist, fail with message: "Need at least 2 tags to generate release notes"
- If no commits between tags, fail with message: "No commits found between tags"
- If GitHub CLI (`gh`) is not installed, fail with installation instructions: "GitHub CLI is required. Install from: https://cli.github.com/"
- If `gh release create` fails, fail with the error message
- If not authenticated with GitHub, fail with message: "Not authenticated with GitHub. Run: gh auth login"
- If workflow fails, display warning but do not fail the command (release was already created)
- If workflow times out after 10 minutes, display warning but do not fail the command
- No user interaction or confirmations - fail fast on any error (except GitHub Actions which should warn only)

## Requirements

- GitHub CLI (`gh`) must be installed and authenticated
- User must have push access to the repository
- HEAD must have a tag before running this command
- At least 2 tags must exist in the repository

## Output Example

```
Creating GitHub release for v1.2.1...

Release Title: v1.2.1 - Fix active flag for PostgreSQL and SurrealDB

Release Notes:
## Improvements
- Remove active column from unversioned model from Postgres and Surreal

---
**Full Changelog**: v1.2.0...v1.2.1

✓ Release created successfully!

Monitoring GitHub Actions workflow...
✓ Workflow completed successfully!

Summary:
📦 Release: v1.2.1
🔗 Release URL: https://github.com/owner/repo/releases/tag/v1.2.1
✅ GitHub Actions: success
🔗 Workflow URL: https://github.com/owner/repo/actions/runs/123456789
```

## Notes

- This command must be run after tagging the release commit
- The release will be marked as "latest" automatically
- GitHub Actions workflows are monitored after release creation
- If GitHub Actions fail, the command will display a warning but will not fail (since the release is already created)
- Uses conventional commit format for categorization (https://www.conventionalcommits.org/)
- Falls back to keyword matching for non-conventional commits
- Merge commits are excluded from release notes
