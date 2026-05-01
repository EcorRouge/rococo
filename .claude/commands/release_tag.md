# Release Tag
> Create and push a git tag based on the version in setup.py

## Instructions

1. Read the setup.py file to extract the current version number
2. Parse the version from the line containing `version='x.y.z'`
3. Verify the tag doesn't already exist (fail if it does)
4. Create a git tag in the format `v{version}` (e.g., v1.2.1)
5. Push the tag to the remote repository
6. Display success message with tag name

## Steps

### 1. Extract Version from setup.py
- Read setup.py and find the line with `version='...'`
- Extract the version number (e.g., "1.2.1")

### 2. Verify Current Git Status
- Check if tag already exists with `git tag -l "v{version}"`
- If tag exists, fail the task immediately with error message

### 3. Create Git Tag
- Create an annotated tag: `git tag -a v{version} -m "Release v{version}"`
- Verify tag was created with `git tag -l "v{version}"`

### 4. Push Tag to Remote
- Push the tag to origin: `git push origin v{version}`
- Command must complete successfully or task fails

### 5. Summary
- Display the tag name that was created and pushed
- Show the command to delete the tag if needed: `git tag -d v{version} && git push origin :refs/tags/v{version}`

## Error Handling

- If version cannot be extracted from setup.py, fail with error message
- If tag already exists locally or remotely, fail with error message
- If tag creation fails, fail with error message
- If push fails, fail with error message
- No user interaction or confirmations - fail fast on any error
