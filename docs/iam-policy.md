# Source IAM policy

Floceed is a read-only source client. Start from the policy below and remove
service sections that are not selected. Resource-level scoping varies by API;
AWS requires `ListAllMyBuckets` and `ListTables` on `*`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConfirmIdentity",
      "Effect": "Allow",
      "Action": "sts:GetCallerIdentity",
      "Resource": "*"
    },
    {
      "Sid": "DiscoverS3",
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets",
        "s3:GetBucketLocation"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ReadSelectedS3",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketTagging",
        "s3:GetBucketVersioning",
        "s3:GetBucketCORS",
        "s3:GetLifecycleConfiguration",
        "s3:GetEncryptionConfiguration",
        "s3:GetBucketPolicy",
        "s3:GetBucketWebsite",
        "s3:GetBucketPublicAccessBlock",
        "s3:GetObjectLockConfiguration",
        "s3:GetBucketNotification",
        "s3:GetReplicationConfiguration",
        "s3:GetBucketLogging",
        "s3:ListBucket",
        "s3:GetObject",
        "s3:GetObjectTagging"
      ],
      "Resource": [
        "arn:aws:s3:::BUCKET",
        "arn:aws:s3:::BUCKET/*"
      ]
    },
    {
      "Sid": "DiscoverDynamoDB",
      "Effect": "Allow",
      "Action": "dynamodb:ListTables",
      "Resource": "*"
    },
    {
      "Sid": "ReadSelectedDynamoDB",
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:DescribeTimeToLive",
        "dynamodb:ListTagsOfResource",
        "dynamodb:Scan"
      ],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/TABLE"
    },
    {
      "Sid": "DiscoverKinesis",
      "Effect": "Allow",
      "Action": "kinesis:ListStreams",
      "Resource": "*"
    },
    {
      "Sid": "ReadSelectedKinesis",
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStreamSummary",
        "kinesis:ListShards",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"
      ],
      "Resource": "arn:aws:kinesis:REGION:ACCOUNT_ID:stream/STREAM"
    },
    {
      "Sid": "ReadSelectedSQS",
      "Effect": "Allow",
      "Action": [
        "sqs:GetQueueUrl",
        "sqs:ReceiveMessage"
      ],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:QUEUE"
    },
    {
      "Sid": "ReadSelectedSNS",
      "Effect": "Allow",
      "Action": "sns:ListSubscriptionsByTopic",
      "Resource": "arn:aws:sns:REGION:ACCOUNT_ID:TOPIC"
    },
    {
      "Sid": "DiscoverEventBridge",
      "Effect": "Allow",
      "Action": "events:ListRules",
      "Resource": "*"
    },
    {
      "Sid": "ReadSelectedEventBridge",
      "Effect": "Allow",
      "Action": "events:ListTargetsByRule",
      "Resource": "arn:aws:events:REGION:ACCOUNT_ID:event-bus/BUS/rule/*"
    },
    {
      "Sid": "ReadSelectedLambda",
      "Effect": "Allow",
      "Action": [
        "lambda:GetFunctionConfiguration",
        "lambda:ListAliases",
        "lambda:ListEventSourceMappings"
      ],
      "Resource": "arn:aws:lambda:REGION:ACCOUNT_ID:function:FUNCTION"
    },
    {
      "Sid": "ReadSelectedSecretsManager",
      "Effect": "Allow",
      "Action": "secretsmanager:DescribeSecret",
      "Resource": "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:SECRET-??????"
    },
    {
      "Sid": "ReadSelectedSSM",
      "Effect": "Allow",
      "Action": "ssm:DescribeParameters",
      "Resource": "*"
    },
    {
      "Sid": "ReadSelectedAPIGateway",
      "Effect": "Allow",
      "Action": "apigateway:GET",
      "Resource": "arn:aws:apigateway:REGION::/apis/*"
    },
    {
      "Sid": "ReadSelectedStepFunctions",
      "Effect": "Allow",
      "Action": [
        "states:DescribeStateMachine",
        "states:ListTagsForResource"
      ],
      "Resource": "arn:aws:states:REGION:ACCOUNT_ID:stateMachine:*"
    },
    {
      "Sid": "DiscoverCloudWatchLogs",
      "Effect": "Allow",
      "Action": "logs:DescribeLogGroups",
      "Resource": "*"
    },
    {
      "Sid": "ReadSelectedCloudWatchLogs",
      "Effect": "Allow",
      "Action": "logs:ListTagsForResource",
      "Resource": "arn:aws:logs:REGION:ACCOUNT_ID:log-group:*"
    }
  ]
}
```

Structure-only projects can omit `s3:GetObject`, `s3:GetObjectTagging`,
`dynamodb:Scan`, `kinesis:ListShards`, `kinesis:GetShardIterator`,
`kinesis:GetRecords`, `sqs:GetQueueUrl`, and `sqs:ReceiveMessage`. Those are
only required when fixture data is enabled for the corresponding resource.
`apigateway:GET` is the read action AWS maps every API Gateway v2 read call to;
its ARN pattern is the API Gateway v2 control-plane resource, not the
`execute-api` invocation resource.

An optional configuration permission denied for one resource is reported as a
finding instead of hiding that resource. The `Discover*` statements cover the
list operations AWS requires on `*`; the `ReadSelected*` statements scope the
per-resource reads to the selected resources. The Kinesis, Lambda, SNS,
EventBridge, Secrets Manager, SSM, Step Functions, API Gateway, and CloudWatch
Logs `ReadSelected*` statements above cover structure-only topology capture;
the SQS statement is required only when SQS message capture is enabled. The
Secrets Manager resource uses `SECRET-??????` so the `-XXXXXX` suffix AWS
appends to every secret ARN is required, matching AWS's recommended
least-privilege pattern. Remove the statements for services you do not select.

Completed S3 reuse evaluates freshness with `ListObjectsV2`, which AWS
authorizes through the existing `s3:ListBucket` action in `ReadSelectedS3`.
It adds no permission beyond the policy above. An unchanged inventory can avoid
`s3:GetObject` body reads during that pull, but data-enabled capture still needs
`s3:GetObject` for first capture and any refreshed unit. DynamoDB does not use
incremental reads or table metadata as a freshness token in v0.4: data-enabled
tables still require `dynamodb:Scan` and are conservatively scanned again.

Fixture profiles do not add AWS permissions. DynamoDB attribute transformation
and cohort selection operate on items already returned by `dynamodb:Scan`; S3
metadata and allowlisted textual-body transformation operate on objects already
returned by `s3:GetObject`. Keep the same least-privilege resource restrictions
and omit data-read actions when fixture data is disabled. The runtime governance
secret is local input and must never be placed in an IAM policy, AWS tag,
project file, or generated bundle.
