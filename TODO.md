# plus-worker TODO

## Completed
- [x] Import feaas-core and plus-engine (via CodeArtifact)
- [x] Add FFMPEG dependency to Docker
- [x] Create worker/actions/ with heavy-compute actions (11 FFMPEG actions)
- [x] Initialize DAO from environment variables
- [x] Load action class dynamically with search paths
- [x] Add feaas-core as dependency
- [x] Add plus-engine as dependency
- [x] **PSEE integration** - complete the job execution loop
  - [x] Use PSEE to actually run the script
  - [x] Update job with progress as execution proceeds (every 60s OR on percent change)
  - [x] Check `request_cancel` on each progress save, abort if true
  - [x] Final state: status=COMPLETED/FAILED, percent=100%, error_count, success_count
  - [x] Stream of Receipts from all individual Actions (handled by PSEE)

## Completed (feaas-core)
- [x] Add fields to PlusScriptJob protobuf: `percent`, `success_count`, `error_count`, `iteration`
  - Published as plus-core 1.0.4
  - Worker updated to require plus-core>=1.0.4

## Completed (plus-engine)
- [x] Streams Fargate support - both collection and stream jobs now launch Fargate tasks
- [x] Unified job_type field: `run_on_collection` or `run_on_stream`
- [x] Worker consolidated to single RUN_JOB mode (reads job_type from job itself)

## Completed (plus-worker iteration)
- [x] Worker loads extra fields from raw job doc (job_type, collection_key, stream_key, hostname, input_data)
- [x] Collection iteration: searches for items, runs script on each, tracks progress
- [x] Stream iteration: same pattern for stream items
- [x] Progress tracking: updates job.percent after each item
- [x] Prominent logging of each object_id being processed
- [x] Test script: test_scripts/test_run_all.py
- [x] Fixed: filter out job documents from collection scan
- [x] Fixed: plus-core 1.0.5 - err_message → error_message typo in PSEE

## Ready to Test
- [x] Deploy and test RUN_JOB mode with a real PlusScriptJob
- [x] Test run_on_collection job type - WORKING
- [ ] Test run_on_stream job type
- [ ] Test FFMPEG action end-to-end with RUN_ACTION mode (start with Probe)

## Future Work
- [ ] Move build/deploy to GitHub Actions
- [ ] Support `oncomplete` callback scripts
- [ ] **Use IAM task roles instead of ACCESS_KEY/SECRET_KEY** - Stop injecting credentials via environment variables; use ECS task execution role with appropriate IAM permissions for DynamoDB, S3, etc.
- [ ] User can pick Fargate compute and memory
- [ ] Unit tests for FFMPEG actions (mock blobstore, verify ffmpeg commands)
- [ ] More specific error handling (file not found, codec errors, disk space)
- [x] Action registry - expose action catalog to plus-engine
- [ ] More vendor actions (ImageMagick, pandoc, etc.)

## Reference

### Fargate CPU/Memory Options
| CPU | Memory Options |
|-----|----------------|
| 256 | 512, 1024, 2048 |
| 512 | 1024-4096 |
| 1024 | 2048-8192 |
| 2048 | 4096-16384 |
| 4096 | 8192-30720 |
