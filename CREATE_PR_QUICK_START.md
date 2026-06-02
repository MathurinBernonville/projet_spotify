# Quick Start: Create Your Pull Request

This is a quick summary. For detailed instructions, see PULL_REQUEST_GUIDE.md

## Your Current Status

- Branch: `groupe-c/data-engineer-batch`
- Target branch: `groupe-c/main`
- Status: Code is committed and pushed

## Create PR in 5 Steps

### Step 1: Go to GitHub

```
https://github.com/courshetic/cours_hetic
```

### Step 2: Click "Compare & pull request"

You should see a yellow banner at the top of the repository:

```
groupe-c/data-engineer-batch had recent pushes 2 minutes ago
[Compare & pull request]  [Dismiss]
```

Click the button.

### Step 3: Fill in PR Title

```
feat(phase-1): implement catalog_ingestion_pipeline DAG
```

### Step 4: Fill in PR Description

Replace with your details:

```markdown
## Description

Implements catalog_ingestion_pipeline DAG with complete functionality:
- Extract JSON files from MinIO
- Validate schema with DLQ for errors
- Transform and normalize data
- Load with idempotent upserts
- Notify with statistics

## What Changed

- dags/catalog_ingestion_pipeline.py: Full implementation (292 lines)
- test_data/: 3 JSON test files
- upload_to_minio.py: Upload script
- start_and_test.sh: Docker automation
- Comprehensive test and PR guides

## How to Test

```bash
./start_and_test.sh
# Then trigger catalog_ingestion_pipeline in Airflow UI
```

## Validation

- [x] All 5 tasks implemented
- [x] Idempotent upserts (ON CONFLICT)
- [x] Error handling via DLQ
- [x] XCom statistics
- [x] Test infrastructure
- [x] Documentation

Closes #4
```

### Step 5: Add Labels

Click "Labels" on right sidebar, add:
- phase-1
- airflow
- dag
- livrable-dag

### Step 6: Create PR

Click "Create pull request" button

## GitHub Will Notify Reviewers

Your PR is now submitted! You will:

1. See automated checks run
2. Receive reviewer comments
3. Be able to push updates if needed

## After Creation

You'll get a URL like:
```
https://github.com/courshetic/cours_hetic/pull/XX
```

Share this link in your team!

## If You Need to Make Changes

```bash
# Edit locally
git add <files>
git commit -m "review: address feedback on validation"
git push origin groupe-c/data-engineer-batch

# The PR automatically updates!
```

## Questions?

See:
- PULL_REQUEST_GUIDE.md (detailed guide)
- TEST_GUIDE.md (testing your changes)
- GitHub PR docs: https://docs.github.com/en/pull-requests
