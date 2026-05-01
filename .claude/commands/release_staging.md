# Release Staging
> Merge main branch to staging and monitor GitHub Actions status

## Instructions

1. Verify current git state and fetch latest changes
2. Checkout staging branch
3. Merge main branch into staging
4. Push staging to remote
5. Wait for GitHub Actions to complete
6. Report final status of GitHub Actions workflow
7. Display summary of the release

## Steps

### 1. Verify Git State
- Fetch latest changes: `git fetch origin`
- Ensure no uncommitted changes exist
- If working directory is dirty, fail the task

### 2. Checkout Staging Branch
- Checkout staging: `git checkout staging`
- Pull latest staging: `git pull origin staging`
- If checkout or pull fails, fail the task

### 3. Merge Main into Staging
- Merge main branch: `git merge origin/main -m "Merge main into staging for release"`
- If merge conflicts occur, fail the task with conflict message
- Verify merge was successful

### 4. Push to Remote
- Push staging to remote: `git push origin staging`
- If push fails, fail the task

### 5. Monitor GitHub Actions
- Use `gh run list --branch staging --limit 1` to get the latest workflow run ID
- Use `gh run watch <run-id>` to monitor the workflow in real-time
- Wait for workflow to complete (success, failure, or cancelled)
- Timeout after 10 minutes if workflow doesn't complete

### 6. Report Status
- Display workflow conclusion (success, failure, cancelled, etc.)
- If workflow failed, show workflow URL: `gh run view <run-id> --web`
- Display summary of what was deployed

### 7. Summary
- Show the merge commit SHA
- Show the GitHub Actions workflow status
- Show staging branch HEAD

## Error Handling

- If git fetch fails, fail with error message
- If working directory is dirty, fail with error message
- If staging branch doesn't exist, fail with error message
- If merge has conflicts, fail with conflict details
- If push fails, fail with error message
- If GitHub CLI (`gh`) is not installed, fail with installation instructions
- If workflow fails, fail the task and show workflow URL
- If workflow times out, fail the task
- No user interaction or confirmations - fail fast on any error

## Requirements

- GitHub CLI (`gh`) must be installed and authenticated
- User must have push access to the repository
- Working directory must be clean before starting
