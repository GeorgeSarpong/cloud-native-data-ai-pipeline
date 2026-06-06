# Cloud-Native Data & AI Pipeline Deployment

![Data Pipeline](https://img.shields.io/badge/Domain-Data%20%26%20AI%20Pipeline-blue)
![AWS Kinesis](https://img.shields.io/badge/Tool-AWS%20Kinesis-orange)
![AWS S3](https://img.shields.io/badge/Tool-AWS%20S3-orange)
![SageMaker](https://img.shields.io/badge/Tool-AWS%20SageMaker-orange)
![MLOps](https://img.shields.io/badge/Framework-MLOps-green)
![Docker](https://img.shields.io/badge/Tool-Docker-blue)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-black)

---

## Project Overview

This project builds and documents a complete cloud-native data and AI pipeline — covering real-time data ingestion using AWS Kinesis and S3, containerised microservices for data processing, AWS SageMaker for predictive analytics and ML model serving, full CI/CD automation, and comprehensive observability across the entire pipeline.

Built to demonstrate operational readiness for:
- Cloud Infrastructure Engineer roles
- Cloud & DevOps Engineer roles
- Data Engineer roles
- MLOps Engineer roles
- AI Infrastructure Engineer roles

---

## Pipeline Capabilities

| Capability | Implementation |
|---|---|
| Real-time data ingestion | AWS Kinesis Data Streams |
| Data storage | AWS S3 Data Lake |
| Data transformation | AWS Glue + Lambda |
| ML model training | AWS SageMaker |
| Model serving | SageMaker Endpoints |
| Model monitoring | SageMaker Model Monitor |
| Pipeline automation | GitHub Actions CI/CD |
| Observability | CloudWatch + Grafana |

---

## Repository Structure

---

## Pipeline Architecture

---

##  AWS Kinesis Configuration

### Kinesis Data Stream Setup

```python
# kinesis_setup.py
import boto3
import json
import time

def create_kinesis_stream():
    kinesis = boto3.client('kinesis', region_name='us-east-1')
    
    # Create manufacturing events stream
    response = kinesis.create_stream(
        StreamName='manufacturing-events',
        ShardCount=4,
        StreamModeDetails={
            'StreamMode': 'PROVISIONED'
        }
    )
    
    print("Stream created successfully")
    
    # Wait for stream to become active
    waiter = kinesis.get_waiter('stream_exists')
    waiter.wait(StreamName='manufacturing-events')
    
    # Add tags
    kinesis.add_tags_to_stream(
        StreamName='manufacturing-events',
        Tags={
            'Environment': 'production',
            'Project': 'data-ai-pipeline',
            'ManagedBy': 'Terraform'
        }
    )
    
    return response

def produce_manufacturing_event(event_data):
    kinesis = boto3.client('kinesis', region_name='us-east-1')
    
    response = kinesis.put_record(
        StreamName='manufacturing-events',
        Data=json.dumps(event_data),
        PartitionKey=event_data['machine_id']
    )
    
    return response

# Example manufacturing event
sample_event = {
    'machine_id': 'MACHINE-001',
    'timestamp': time.time(),
    'temperature': 75.5,
    'pressure': 120.3,
    'vibration': 0.05,
    'production_rate': 850,
    'error_code': None,
    'location': 'plant-floor-a'
}

produce_manufacturing_event(sample_event)
```

### Kinesis Firehose to S3

```python
# kinesis_firehose_setup.py
import boto3

def create_firehose_delivery_stream():
    firehose = boto3.client('firehose', region_name='us-east-1')
    
    response = firehose.create_delivery_stream(
        DeliveryStreamName='manufacturing-to-s3',
        DeliveryStreamType='KinesisStreamAsSource',
        KinesisStreamSourceConfiguration={
            'KinesisStreamARN': 'arn:aws:kinesis:us-east-1:ACCOUNT:stream/manufacturing-events',
            'RoleARN': 'arn:aws:iam::ACCOUNT:role/firehose-role'
        },
        S3DestinationConfiguration={
            'RoleARN': 'arn:aws:iam::ACCOUNT:role/firehose-role',
            'BucketARN': 'arn:aws:s3:::manufacturing-data-lake',
            'Prefix': 'raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/',
            'ErrorOutputPrefix': 'errors/',
            'BufferingHints': {
                'SizeInMBs': 128,
                'IntervalInSeconds': 300
            },
            'CompressionFormat': 'GZIP',
            'EncryptionConfiguration': {
                'KMSEncryptionConfig': {
                    'AWSKMSKeyARN': 'arn:aws:kms:us-east-1:ACCOUNT:key/KEY-ID'
                }
            }
        }
    )
    
    return response
```

---

## Lambda Processing Function

```python
# lambda_processor.py
import boto3
import json
import base64
from datetime import datetime

def lambda_handler(event, context):
    """
    Process manufacturing events from Kinesis stream
    Validate, transform, and store to processed S3 zone
    """
    
    s3 = boto3.client('s3')
    processed_records = []
    failed_records = []
    
    for record in event['Records']:
        try:
            # Decode Kinesis record
            raw_data = base64.b64decode(record['kinesis']['data'])
            event_data = json.loads(raw_data)
            
            # Validate required fields
            required_fields = ['machine_id', 'timestamp', 'temperature', 'pressure']
            if not all(field in event_data for field in required_fields):
                raise ValueError(f"Missing required fields in record")
            
            # Transform and enrich data
            processed_record = {
                'machine_id': event_data['machine_id'],
                'timestamp': event_data['timestamp'],
                'processed_at': datetime.utcnow().isoformat(),
                'temperature_celsius': event_data['temperature'],
                'pressure_psi': event_data['pressure'],
                'vibration_g': event_data.get('vibration', 0),
                'production_rate': event_data.get('production_rate', 0),
                'status': 'normal' if not event_data.get('error_code') else 'error',
                'anomaly_score': calculate_anomaly_score(event_data)
            }
            
            processed_records.append(processed_record)
            
        except Exception as e:
            failed_records.append({
                'record': record,
                'error': str(e)
            })
    
    # Store processed records to S3
    if processed_records:
        timestamp = datetime.utcnow()
        key = f"processed/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/{timestamp.timestamp()}.json"
        
        s3.put_object(
            Bucket='manufacturing-data-lake',
            Key=key,
            Body=json.dumps(processed_records),
            ContentType='application/json'
        )
    
    print(f"Processed: {len(processed_records)}, Failed: {len(failed_records)}")
    
    return {
        'statusCode': 200,
        'processed': len(processed_records),
        'failed': len(failed_records)
    }

def calculate_anomaly_score(data):
    """Simple anomaly scoring based on thresholds"""
    score = 0
    
    if data.get('temperature', 0) > 85:
        score += 0.4
    if data.get('pressure', 0) > 150:
        score += 0.3
    if data.get('vibration', 0) > 0.1:
        score += 0.3
        
    return round(score, 2)
```

---

## AWS SageMaker — Predictive Analytics

### Model Training Job

```python
# sagemaker_training.py
import boto3
import sagemaker
from sagemaker.sklearn.estimator import SKLearn

def train_predictive_maintenance_model():
    """
    Train predictive maintenance model using manufacturing data
    Predicts equipment failure before it occurs
    """
    
    session = sagemaker.Session()
    role = 'arn:aws:iam::ACCOUNT:role/sagemaker-role'
    
    # Define SKLearn estimator
    sklearn_estimator = SKLearn(
        entry_point='train.py',
        source_dir='./training',
        role=role,
        instance_type='ml.m5.xlarge',
        instance_count=1,
        framework_version='1.0-1',
        py_version='py3',
        hyperparameters={
            'n-estimators': 100,
            'max-depth': 10,
            'min-samples-split': 5,
            'test-size': 0.2
        },
        metric_definitions=[
            {'Name': 'validation:accuracy', 'Regex': 'Accuracy: ([0-9\\.]+)'},
            {'Name': 'validation:f1', 'Regex': 'F1 Score: ([0-9\\.]+)'}
        ]
    )
    
    # Start training job
    sklearn_estimator.fit(
        inputs={
            'train': 's3://manufacturing-data-lake/curated/training/',
            'validation': 's3://manufacturing-data-lake/curated/validation/'
        },
        job_name=f'predictive-maintenance-{int(time.time())}'
    )
    
    return sklearn_estimator

def deploy_model(estimator):
    """Deploy trained model to SageMaker endpoint"""
    
    predictor = estimator.deploy(
        initial_instance_count=1,
        instance_type='ml.t2.medium',
        endpoint_name='predictive-maintenance-endpoint',
        serializer=sagemaker.serializers.JSONSerializer(),
        deserializer=sagemaker.deserializers.JSONDeserializer()
    )
    
    return predictor
```

### Model Inference API

```python
# prediction_service.py
import boto3
import json
from flask import Flask, request, jsonify

app = Flask(__name__)
sagemaker_runtime = boto3.client('sagemaker-runtime')

@app.route('/predict', methods=['POST'])
def predict():
    """
    Real-time prediction endpoint
    Accepts machine sensor data and returns failure probability
    """
    
    try:
        data = request.get_json()
        
        # Validate input
        required_fields = ['machine_id', 'temperature', 'pressure', 'vibration']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Prepare features for model
        features = [
            data['temperature'],
            data['pressure'],
            data['vibration'],
            data.get('production_rate', 0),
            data.get('operating_hours', 0)
        ]
        
        # Call SageMaker endpoint
        response = sagemaker_runtime.invoke_endpoint(
            EndpointName='predictive-maintenance-endpoint',
            ContentType='application/json',
            Body=json.dumps({'features': features})
        )
        
        result = json.loads(response['Body'].read())
        
        return jsonify({
            'machine_id': data['machine_id'],
            'failure_probability': result['probability'],
            'prediction': result['prediction'],
            'confidence': result['confidence'],
            'recommendation': get_recommendation(result['probability'])
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_recommendation(probability):
    """Generate maintenance recommendation based on failure probability"""
    if probability > 0.8:
        return 'IMMEDIATE_MAINTENANCE_REQUIRED'
    elif probability > 0.6:
        return 'SCHEDULE_MAINTENANCE_WITHIN_24H'
    elif probability > 0.4:
        return 'MONITOR_CLOSELY'
    else:
        return 'NORMAL_OPERATION'

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

---

## MLOps — Model Retraining Pipeline

```yaml
# GitHub Actions — Model Retraining Pipeline
name: MLOps Model Retraining Pipeline

on:
  schedule:
    - cron: '0 2 * * 0'  # Weekly retraining — Sunday 2am
  workflow_dispatch:      # Manual trigger
    inputs:
      reason:
        description: 'Reason for manual retraining'
        required: true

jobs:
  data-validation:
    name: Validate Training Data
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: pip install boto3 pandas scikit-learn great-expectations

      - name: Validate data quality
        run: python scripts/validate_data.py
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}

  model-training:
    name: Train New Model
    runs-on: ubuntu-latest
    needs: data-validation
    steps:
      - uses: actions/checkout@v3

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1

      - name: Start SageMaker training job
        run: python scripts/train_model.py

  model-evaluation:
    name: Evaluate Model Performance
    runs-on: ubuntu-latest
    needs: model-training
    steps:
      - name: Compare with production model
        run: python scripts/evaluate_model.py

      - name: Gate — Accuracy must exceed 90%
        run: python scripts/accuracy_gate.py

  model-deployment:
    name: Deploy to Production
    runs-on: ubuntu-latest
    needs: model-evaluation
    environment: production
    steps:
      - name: Register model in model registry
        run: python scripts/register_model.py

      - name: Update SageMaker endpoint
        run: python scripts/deploy_model.py

      - name: Run inference tests
        run: python scripts/test_inference.py

      - name: Enable model monitoring
        run: python scripts/setup_monitoring.py
```

---

## Data Quality Monitoring

```python
# data_quality_checks.py
import boto3
import pandas as pd
from datetime import datetime, timedelta

def run_data_quality_checks():
    """
    Run comprehensive data quality checks on ingested data
    Alert if quality drops below threshold
    """
    
    s3 = boto3.client('s3')
    cloudwatch = boto3.client('cloudwatch')
    
    # Load latest data partition
    yesterday = (datetime.utcnow() - timedelta(days=1))
    prefix = f"processed/year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/"
    
    # Get all files for yesterday
    response = s3.list_objects_v2(
        Bucket='manufacturing-data-lake',
        Prefix=prefix
    )
    
    records = []
    for obj in response.get('Contents', []):
        file_response = s3.get_object(
            Bucket='manufacturing-data-lake',
            Key=obj['Key']
        )
        data = json.loads(file_response['Body'].read())
        records.extend(data)
    
    df = pd.DataFrame(records)
    
    # Run quality checks
    quality_metrics = {
        'total_records': len(df),
        'null_percentage': df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100,
        'duplicate_percentage': df.duplicated().sum() / len(df) * 100,
        'temperature_out_of_range': len(df[df['temperature_celsius'] > 100]) / len(df) * 100,
        'anomaly_rate': len(df[df['anomaly_score'] > 0.7]) / len(df) * 100
    }
    
    # Publish metrics to CloudWatch
    for metric_name, value in quality_metrics.items():
        cloudwatch.put_metric_data(
            Namespace='ManufacturingPipeline/DataQuality',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count' if metric_name == 'total_records' else 'Percent',
                'Timestamp': datetime.utcnow()
            }]
        )
    
    # Alert if quality below threshold
    if quality_metrics['null_percentage'] > 5:
        send_quality_alert('High null rate detected', quality_metrics)
    
    if quality_metrics['duplicate_percentage'] > 2:
        send_quality_alert('High duplicate rate detected', quality_metrics)
    
    print(f"Data quality checks complete: {quality_metrics}")
    return quality_metrics

def send_quality_alert(message, metrics):
    sns = boto3.client('sns')
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:ACCOUNT:data-quality-alerts',
        Subject=f'Data Quality Alert — {message}',
        Message=json.dumps(metrics, indent=2)
    )
```

---

## CloudWatch & Grafana Monitoring

### Pipeline Metrics Dashboard

| Panel | Metric | Visualization |
|---|---|---|
| Records Ingested | Kinesis records/second | Time series |
| Processing Latency | Lambda duration | Time series |
| Pipeline Errors | Lambda error rate | Alert list |
| Model Predictions | Predictions/minute | Counter |
| Model Accuracy | Rolling accuracy % | Gauge |
| Data Quality Score | Quality checks % | Gauge |
| S3 Storage Used | Bytes stored | Stat |
| SageMaker Endpoint Latency | Inference time ms | Time series |

---

## Security Configuration

### Data Encryption

```hcl
# KMS Key for data lake encryption
resource "aws_kms_key" "data_lake" {
  description             = "KMS key for manufacturing data lake"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = {
    Name        = "data-lake-kms-key"
    Environment = "production"
    ManagedBy   = "Terraform"
  }
}

# S3 bucket with encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

# S3 bucket versioning
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}
```

---

## Standards & Frameworks Referenced

- **AWS Well-Architected Framework** — Data Analytics Lens
- **MLOps Principles** — Model lifecycle management
- **FinOps Foundation** — Data pipeline cost optimisation
- **NIST SP 800-53** — Data security controls
- **GDPR** — Data privacy and protection
- **ISO/IEC 27001** — Information security management

---

## Tools & Technologies

![Kinesis](https://img.shields.io/badge/AWS-Kinesis-orange)
![S3](https://img.shields.io/badge/AWS-S3%20Data%20Lake-orange)
![Lambda](https://img.shields.io/badge/AWS-Lambda-orange)
![Glue](https://img.shields.io/badge/AWS-Glue-orange)
![SageMaker](https://img.shields.io/badge/AWS-SageMaker-orange)
![Docker](https://img.shields.io/badge/Docker-Containerization-blue)
![GitHub Actions](https://img.shields.io/badge/GitHub-Actions-black)
![CloudWatch](https://img.shields.io/badge/CloudWatch-Monitoring-yellow)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-orange)
![Python](https://img.shields.io/badge/Python-Primary-blue)

---

## Author

**George Amankwaa Sarpong**
Cloud Infrastructure & DevOps Engineer | AI/ML Operations
📍 Accra, Ghana 
🔗 [LinkedIn](https://linkedin.com/in/georgesarpong)
🌐 [GitHub Portfolio](https://github.com/GeorgeSarpong)

---

*This project is part of a broader portfolio demonstrating readiness for Cloud Infrastructure Engineer, Cloud DevOps Engineer, and AI Infrastructure roles in the US and global market.*
