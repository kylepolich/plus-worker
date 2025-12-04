# plus-worker TODO

## Completed
- [x] Writes receipts to stream_id="{hostname}/{username}/stream-run-all.{uuid}" after each item
- [x] Update node support: applies field mappings to update original records after action completes
- [x] Receipt Aggregator: tracks success_count/error_count on job

## Ready to Test
- [ ] Test run_on_stream job type
- [ ] Test FFMPEG action end-to-end with RUN_ACTION mode (start with Probe)
- [ ] Test Update node field mappings (e.g., Output->Label)

## Future Work
- [ ] **Batch updates**: Currently updates records one-by-one after each action. Consider batching for better performance. See TODO comments in worker.py `apply_update_mappings()` and `run_on_collection()`
- [ ] **Batch receipt writes**: Consider batching stream writes instead of one-by-one
- [ ] Fix IAM user confusion: plus-engine uses `feaas-py` to launch Fargate tasks, not `feaas-core-ci-cd`. Need to properly document which IAM user needs which permissions.
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
