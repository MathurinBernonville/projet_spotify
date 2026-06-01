# How to Create a Pull Request for catalog_ingestion_pipeline

This guide explains step-by-step how to create a Pull Request on GitHub to submit your work.

## Overview

A Pull Request (PR) is how you propose changes to a repository. It allows reviewers to check your code before it's merged into the main branch.

## Prerequisites

- Your code is committed and pushed to a feature branch (`groupe-c/data-engineer-batch`)
- You have write access to the repository
- GitHub account access

## Step 1: Verify Your Branch and Commits

First, verify that your work is properly committed and pushed:

```bash
# Check current branch
git branch

# Output should show:
#   groupe-c/data-engineer-batch (with * marking current branch)

# Verify commits are pushed
git log --oneline -5

# Output should show your commits

# Verify remote has your branch
git push origin groupe-c/data-engineer-batch
```

## Step 2: Open GitHub Repository

1. Go to: https://github.com/courshetic/cours_hetic
2. You should see a notification about your recently pushed branch

## Step 3: Create Pull Request via GitHub UI

### Option A: Using GitHub Notification (Easiest)

1. Go to the repository home page
2. You will see a yellow banner saying:
   ```
   groupe-c/data-engineer-batch had recent pushes 2 minutes ago
   [Compare & pull request]
   ```
3. Click the "Compare & pull request" button
4. Skip to Step 4

### Option B: Manual PR Creation

1. Click the "Pull requests" tab at the top of the repository
2. Click the "New pull request" button
3. Set "base" branch to `groupe-c/main` (your target branch)
4. Set "compare" branch to `groupe-c/data-engineer-batch` (your feature branch)
5. Click "Create pull request"

## Step 4: Fill in PR Details

### Title
Use a clear, descriptive title:

```
feat(phase-1): implement catalog_ingestion_pipeline DAG
```

Format: `type(scope): description`

Types:
- `feat`: New feature (like your implementation)
- `fix`: Bug fix
- `docs`: Documentation
- `test`: Tests
- `chore`: Maintenance

### Description

Provide a comprehensive description. Copy and adapt this template:

```markdown
## Description

This PR implements the catalog_ingestion_pipeline DAG (#4) with complete functionality for ingesting musical metadata from JSON files stored in MinIO.

## Changes

- Implemented extract_from_minio() to download 3 JSON files from MinIO
- Implemented validate_schema() with mandatory field validation
- Implemented transform_catalog() with artist name normalization
- Implemented load_to_postgres() with idempotent upserts using ON CONFLICT
- Implemented notify_success() with XCom statistics push
- Added comprehensive test infrastructure with sample data
- Added Docker automation scripts and testing guide

## Files Changed

- dags/catalog_ingestion_pipeline.py: Full DAG implementation (292 lines)
- test_data/: 3 JSON files with test data (9 artists, 9 albums, 12 tracks)
- upload_to_minio.py: Script to upload test data to MinIO
- start_and_test.sh: Automated Docker setup script
- TEST_GUIDE.md: Complete testing documentation

## Testing

To test locally:

```bash
./start_and_test.sh
open http://localhost:8080  # admin/admin
# Trigger catalog_ingestion_pipeline DAG
# Verify green run and XCom statistics
```

Expected results:
- First run: 12 tracks inserted, 9 artists inserted
- Second run (idempotence): Same results

## Validation Checklist

- [x] DAG loads without syntax errors
- [x] All 5 tasks implemented (no NotImplementedError)
- [x] Upserts are idempotent (ON CONFLICT DO UPDATE)
- [x] Error handling via DLQ for invalid records
- [x] XCom push for monitoring statistics
- [x] Comprehensive logging
- [x] Test data included
- [x] Documentation complete

## Related Issue

Closes #4

## Screenshots (Optional)

If you want to include screenshots:
- Airflow UI showing DAG run in green
- Database results showing 12 inserted tracks

## Reviewers

Consider tagging the code owner for review:
@Ossama-65
```

## Step 5: Add Labels and Assignees

1. On the right sidebar, click "Labels"
   - Add: `phase-1`, `airflow`, `dag`, `livrable-dag`

2. Click "Assignees"
   - Assign to yourself initially, can be changed by reviewers

3. Click "Milestone" (if available)
   - Select: `Phase 1 - Batch Airflow`

## Step 6: Create the Pull Request

1. Review all information one more time
2. Ensure branch comparison shows your changes
3. Click "Create pull request" button

## Step 7: Wait for Checks and Reviews

### GitHub Actions / CI Checks

If the repository has automated tests:
- Tests will run automatically
- Check for green checkmarks or red X marks
- If tests fail, see section "Fix Failing Tests"

### Code Review

1. Reviewers will add comments on specific lines
2. You'll receive notifications
3. Address feedback by:
   - Making code changes in your local branch
   - Committing and pushing updates
   - Adding replies to comments

## Step 8: Respond to Feedback

### If Changes Are Requested

```bash
# Make changes locally
vim dags/catalog_ingestion_pipeline.py

# Commit with descriptive message
git commit -am "review: address feedback on error handling"

# Push to update the PR
git push origin groupe-c/data-engineer-batch
```

The PR will automatically update with your new commits.

### Reply to Comments

1. In the PR, find the comment on the specific line
2. Click "Reply" and explain your changes
3. Click "Comment" to submit

### Mark as Ready

If you made changes:
1. Check if "Conversation" tab shows any pending reviews
2. Reply to all comments
3. Click "Resolve" on conversations you've addressed

## Step 9: Approval and Merge

### When PR is Approved

1. PR will show "Approved" status
2. "Merge pull request" button becomes green
3. Repository maintainer will merge (usually you need maintainer approval)

### Merging Options

Three merge strategies typically available:

1. **Create a merge commit** (recommended for main branches)
   ```
   Commits: Keep all commits from your branch
   ```

2. **Squash and merge** (for cleaner history)
   ```
   Combines all commits into one
   ```

3. **Rebase and merge** (if preferred by team)
   ```
   Replays commits on top of base branch
   ```

### To Merge

1. Select merge strategy
2. Click "Confirm merge"
3. Optionally "Delete branch" after merge

## Step 10: Verify Merge

After merge:

```bash
# Switch to main branch
git checkout groupe-c/main

# Update local branch
git pull origin groupe-c/main

# Verify your commits are there
git log --oneline -5
```

## Common Issues and Solutions

### "Cannot Create Pull Request - Branch is Behind"

```bash
# Update your branch with latest changes
git fetch origin
git rebase origin/groupe-c/main

# Force push if needed
git push origin groupe-c/data-engineer-batch --force-with-lease
```

### "Merge Conflict"

If changes were made to the same files:

```bash
# Pull latest changes
git fetch origin

# Rebase your branch
git rebase origin/groupe-c/main

# Fix conflicts in editor
# Mark as resolved
git add <files>

# Continue rebase
git rebase --continue

# Push
git push origin groupe-c/data-engineer-batch --force-with-lease
```

### Tests Failing

1. Read the test output in the "Checks" tab
2. Make necessary fixes locally
3. Push commits
4. Tests will re-run automatically

### "Requires Review from Code Owners"

1. PR cannot be merged until approved by someone in CODEOWNERS file
2. Request review from appropriate person
3. Wait for their feedback

## Best Practices

1. **Keep PRs focused**: One feature per PR, not multiple unrelated changes
2. **Write clear commit messages**: Help reviewers understand your changes
3. **Keep commits atomic**: Each commit should be a logical unit
4. **Test before submitting**: Run tests locally first
5. **Respond promptly**: Answer reviewer questions quickly
6. **Don't force push after review starts**: Unless explicitly asked

## Example PR Workflow

```
1. Create feature branch
   git checkout -b groupe-c/feature-name

2. Make changes and commit
   git commit -m "feat: implement feature"

3. Push to GitHub
   git push origin groupe-c/feature-name

4. Create PR on GitHub UI
   - Title: feat(scope): description
   - Description: Detailed explanation
   - Labels: phase-1, livrable-dag
   - Milestone: Phase 1

5. Wait for automated checks
   - If failing: Fix and push

6. Request review from team
   - Comment: "Ready for review @reviewer"

7. Address feedback
   - Make changes, commit, push

8. Merge when approved
   - Maintainer clicks "Merge pull request"

9. Delete feature branch
   - Click "Delete branch" button
```

## Resources

- GitHub PR Documentation: https://docs.github.com/en/pull-requests
- Creating a Pull Request: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request
- PR Best Practices: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/collaborating-on-repositories/about-pull-requests

## Quick Reference

```bash
# Typical workflow
git checkout -b groupe-c/feature
# Make changes
git add .
git commit -m "feat(scope): description"
git push origin groupe-c/feature

# Then create PR on GitHub UI
```

## Support

If you have questions:
- Check the PR template on GitHub
- Look at previous PRs in the repository
- Ask the team in Discord/Slack
