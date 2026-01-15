# Release Notes
> Generate release notes from commit messages between the current and previous git tags

## Instructions

1. Get the current (latest) tag from the repository
2. Get the previous tag (second latest)
3. Collect all commit messages between these two tags
4. Parse and categorize commits into: New Features, Bug Fixes, Improvements
5. Generate a formatted bullet-point summary
6. Display the release notes

## Steps

### 1. Get Current and Previous Tags
- List all tags sorted by version: `git tag --sort=-version:refname`
- Extract the latest tag (current release)
- Extract the second latest tag (previous release)
- If less than 2 tags exist, fail with error message

### 2. Collect Commits Between Tags
- Get commit messages: `git log <previous-tag>..<current-tag> --pretty=format:"%s" --no-merges`
- Exclude merge commits to focus on actual changes
- If no commits found, fail with message

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

### 4. Format Release Notes
Generate output in the following format:

```
# Release Notes: v{current-tag}
Previous version: v{previous-tag}
Commits: {count}

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
```

### 5. Clean Up Commit Messages
- Remove conventional commit prefixes (feat:, fix:, etc.) from the output
- Capitalize first letter of each bullet point
- Remove unnecessary whitespace
- Keep messages concise (truncate if longer than 100 characters)
- Remove issue numbers and PR references from the end (e.g., (#123), GH-456)

### 6. Display Release Notes
- Print the formatted release notes to stdout
- Include the tag range at the top
- Show commit count for each category
- Omit empty categories (don't show sections with 0 commits)

## Error Handling

- If git repository is not found, fail with error message
- If less than 2 tags exist, fail with message: "Need at least 2 tags to generate release notes"
- If no commits between tags, fail with message: "No commits found between {prev} and {current}"
- If git commands fail, fail with error message
- No user interaction or confirmations - fail fast on any error

## Output Example

```
# Release Notes: v1.2.1
Previous version: v1.2.0
Total commits: 15

## New Features (5)
- Add dark mode toggle to settings
- Implement user authentication with OAuth
- Add export functionality for reports
- Introduce new dashboard layout
- Add support for custom themes

## Bug Fixes (7)
- Resolve memory leak in data processing
- Correct timezone handling for timestamps
- Fix crash when loading empty datasets
- Resolve UI alignment issues on mobile
- Fix duplicate entries in search results
- Correct calculation error in statistics
- Fix broken links in documentation

## Improvements (3)
- Optimize database query performance
- Update dependencies to latest versions
- Refactor authentication module for better maintainability
```

## Notes

- Uses conventional commit format for categorization (https://www.conventionalcommits.org/)
- Falls back to keyword matching for non-conventional commits
- Prioritizes explicit prefixes over keyword matching
- Merge commits are excluded from release notes

## Report

Return ONLY the release notes generated (no other text, no summarization)
