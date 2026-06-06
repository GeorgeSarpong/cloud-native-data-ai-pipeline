# Data Pipeline Failure Response Runbook

**Document Type:** Incident Response Runbook
**Severity:** P1 — Critical
**Author:** George Amankwaa Sarpong
**Last Updated:** June 2026

---

## Purpose
Standardised procedure for detecting, diagnosing, and resolving data pipeline failures ensuring data continuity and minimal data loss.

---

## Failure Categories

| Failure Type | Severity | Examples |
|---|---|---|
| Complete pipeline down | P1 | Kinesis stream down, Lambda throttling |
| Data processing errors | P2 | Schema mismatch, transformation failures |
| Model serving failure | P1 | SageMaker endpoint down |
| Data quality degradation | P2 | High null rate, anomaly spike |
| Storage issues | P2 | S3 bucket access denied |

---

## Response Procedure

### Phase 1 — Detection (0–2 minutes)
1. Acknowledge CloudWatch alarm
2. Create incident ticket with timestamp
3. Identify which pipeline component failed
4. Assess data loss risk and business impact
5. Notify on-call data engineer

### Phase 2 — Diagnosis (2–15 minutes)

#### Kinesis Stream Issues
```bash
# Check stream status
aws kinesis describe-stream-summary \
  --stream-name manufacturing-events

# Check iterator age (data lag)
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name GetRecords.IteratorAgeMilliseconds \
  --dimensions Name=StreamName,Value=manufacturing-events \
  --start-time $(date -u -v-1H +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Maximum
```

#### Lambda Processing Issues
```bash
# Check Lambda error rate
aws logs filter-log-events \
  --log-group-name /aws/lambda/manufacturing-processor \
  --filter-pattern "ERROR" \
  --start-time $(date -d '-1 hour' +%s000)

# Check Lambda throttling
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Throttles \
  --dimensions Name=FunctionName,Value=manufacturing-processor \
  --start-time $(date -u -v-1H +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 60 \
  --statistics Sum
```

#### SageMaker Endpoint Issues
```bash
# Check endpoint status
aws sagemaker describe-endpoint \
  --endpoint-name predictive-maintenance-endpoint

# Check endpoint invocation errors
aws cloudwatch get-metric-statistics \
  --namespace AWS/SageMaker \
  --metric-name Invocation4XXErrors \
  --dimensions Name=EndpointName,Value=predictive-maintenance-endpoint \
  --start-time $(date -u -v-1H +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

### Phase 3 — Resolution

#### Kinesis Recovery
1. If stream in CREATING state — wait for completion
2. If throughput exceeded — increase shard count
3. If consumer failing — check Lambda logs and fix code
4. Replay missed records from iterator position

#### Lambda Recovery
1. If timeout — increase timeout setting
2. If memory — increase memory allocation
3. If throttling — request concurrency limit increase
4. If code error — fix and redeploy

#### SageMaker Recovery
1. If endpoint down — check instance health
2. Restart endpoint if unhealthy
3. If model error — rollback to previous model version
4. Monitor endpoint health after restart

### Phase 4 — Data Recovery
1. Identify data gap time range
2. Replay Kinesis records from sequence number
3. Reprocess failed S3 objects
4. Validate data completeness after recovery
5. Update monitoring to prevent recurrence

---

## Escalation Matrix

| Timeline | Action | Contact |
|---|---|---|
| 0 minutes | Acknowledge alert | On-call Data Engineer |
| 5 minutes | Diagnose and notify | Data Engineering Lead |
| 15 minutes | Vendor support if needed | AWS Support |
| 30 minutes | Executive notification | Engineering Manager |

---

## Post-Incident Actions
- Complete incident report within 24 hours
- Root cause analysis within 48 hours
- Update pipeline monitoring thresholds
- Add automated recovery procedures if possible
- Review and update this runbook
