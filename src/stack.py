from typing import cast

from aws_cdk import (
    Aws,
    CfnOutput,
    Duration,
    Environment,
    RemovalPolicy,
    Stack,
    Tags,
)
from aws_cdk import (
    aws_cloudwatch as cloudwatch,
)
from aws_cdk import (
    aws_events as events,
)
from aws_cdk import (
    aws_events_targets as events_targets,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_kms as kms,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_sqs as sqs,
)
from cdk_nag import NagSuppressions
from constructs import Construct
from src.config import AppConfig


class DataQualityStack(Stack):
    """Secure storage and event-delivery foundation for the data-quality platform."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: AppConfig,
        env: Environment | None = None,
    ) -> None:
        stack_name = f"{config.project_name}-{config.environment.value}"
        super().__init__(
            scope,
            construct_id,
            description="Manifest MedEx secure data-quality ingestion foundation",
            env=env,
            stack_name=stack_name,
            termination_protection=config.termination_protection,
        )

        self._add_standard_tags(config)

        self.encryption_key = kms.Key(
            self,
            "EncryptionKey",
            alias=f"alias/{stack_name}",
            description="Encrypts Manifest MedEx data and queue messages",
            enable_key_rotation=True,
            pending_window=Duration.days(30),
            removal_policy=RemovalPolicy.RETAIN,
        )
        self._allow_event_delivery_to_use_key()

        self.access_logs_bucket = s3.Bucket(
            self,
            "AccessLogsBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            minimum_tls_version=1.2,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=True,
        )

        # The access-log destination cannot recursively log to itself.
        NagSuppressions.add_resource_suppressions(
            self.access_logs_bucket,
            [
                {
                    "id": "AwsSolutions-S1",
                    "reason": "The dedicated access-log destination cannot log to itself.",
                }
            ],
        )

        self.raw_bucket = self._create_data_bucket("RawBucket", "raw/")
        raw_bucket_resource = cast(s3.CfnBucket, self.raw_bucket.node.default_child)
        raw_bucket_resource.notification_configuration = (
            s3.CfnBucket.NotificationConfigurationProperty(
                event_bridge_configuration=s3.CfnBucket.EventBridgeConfigurationProperty(
                    event_bridge_enabled=True
                )
            )
        )
        self.parsed_bucket = self._create_data_bucket("ParsedBucket", "parsed/")
        self.error_bucket = self._create_data_bucket("ErrorBucket", "error/")

        self.parse_queue, self.parse_dead_letter_queue = self._create_queue_pair(
            "Parse",
            visibility_timeout=Duration.minutes(15),
        )
        self.index_queue, self.index_dead_letter_queue = self._create_queue_pair(
            "Index",
            visibility_timeout=Duration.minutes(5),
        )

        self.raw_object_rule = events.Rule(
            self,
            "RawObjectRule",
            description="Routes newly created raw objects to the parse queue",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={"bucket": {"name": [self.raw_bucket.bucket_name]}},
            ),
        )
        self.raw_object_rule.add_target(
            events_targets.SqsQueue(
                self.parse_queue,
                dead_letter_queue=self.parse_dead_letter_queue,
                max_event_age=Duration.hours(2),
                retry_attempts=10,
            )
        )

        # creates CloudWatch alarms for SQS queue
        self._create_queue_alarms("Parse", self.parse_queue, self.parse_dead_letter_queue)
        self._create_queue_alarms("Index", self.index_queue, self.index_dead_letter_queue)
        self._create_outputs()

    def _add_standard_tags(self, config: AppConfig) -> None:
        tags = {
            "Application": config.project_name,
            "DataClassification": "PHI",
            "Environment": config.environment.value,
            "ManagedBy": "AWS-CDK",
        }
        for key, value in tags.items():
            Tags.of(self).add(key, value)

    def _allow_event_delivery_to_use_key(self) -> None:
        for service_principal, statement_id in (
            ("s3.amazonaws.com", "AllowS3EventNotifications"),
            ("events.amazonaws.com", "AllowEventBridgeDelivery"),
        ):
            self.encryption_key.add_to_resource_policy(
                iam.PolicyStatement(
                    sid=statement_id,
                    actions=["kms:Decrypt", "kms:GenerateDataKey"],
                    principals=[iam.ServicePrincipal(service_principal)],
                    resources=["*"],
                    conditions={"StringEquals": {"aws:SourceAccount": Aws.ACCOUNT_ID}},
                )
            )
        NagSuppressions.add_resource_suppressions(
            self.encryption_key,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "KMS key policies require a wildcard resource; access is constrained to "
                        "AWS event-delivery services in this AWS account."
                    ),
                }
            ],
        )

    def _create_data_bucket(self, construct_id: str, log_prefix: str) -> s3.Bucket:
        return s3.Bucket(
            self,
            construct_id,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            bucket_key_enabled=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.encryption_key,
            enforce_ssl=True,
            minimum_tls_version=1.2,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix=log_prefix,
            versioned=True,
        )

    def _create_queue_pair(
        self,
        construct_id: str,
        *,
        visibility_timeout: Duration,
    ) -> tuple[sqs.Queue, sqs.Queue]:
        dead_letter_queue = sqs.Queue(
            self,
            f"{construct_id}DeadLetterQueue",
            data_key_reuse=Duration.minutes(5),
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.encryption_key,
            enforce_ssl=True,
            retention_period=Duration.days(14),
        )
        queue = sqs.Queue(
            self,
            f"{construct_id}Queue",
            data_key_reuse=Duration.minutes(5),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=5,
                queue=dead_letter_queue,
            ),
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.encryption_key,
            enforce_ssl=True,
            retention_period=Duration.days(14),
            visibility_timeout=visibility_timeout,
        )
        return queue, dead_letter_queue

    def _create_queue_alarms(
        self,
        construct_id: str,
        queue: sqs.Queue,
        dead_letter_queue: sqs.Queue,
    ) -> None:
        cloudwatch.Alarm(
            self,
            f"{construct_id}QueueAgeAlarm",
            alarm_description=(
                f"{construct_id.lower()} queue has unprocessed messages older than 15 minutes"
            ),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            datapoints_to_alarm=1,
            evaluation_periods=1,
            metric=queue.metric_approximate_age_of_oldest_message(
                period=Duration.minutes(5),
                statistic="Maximum",
            ),
            threshold=900,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        cloudwatch.Alarm(
            self,
            f"{construct_id}DeadLetterQueueAlarm",
            alarm_description=f"{construct_id.lower()} dead-letter queue contains failed messages",
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            datapoints_to_alarm=1,
            evaluation_periods=1,
            metric=dead_letter_queue.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(5),
                statistic="Maximum",
            ),
            threshold=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

    def _create_outputs(self) -> None:
        outputs = {
            "EncryptionKeyArn": self.encryption_key.key_arn,
            "RawBucketName": self.raw_bucket.bucket_name,
            "ParsedBucketName": self.parsed_bucket.bucket_name,
            "ErrorBucketName": self.error_bucket.bucket_name,
            "ParseQueueUrl": self.parse_queue.queue_url,
            "IndexQueueUrl": self.index_queue.queue_url,
        }
        for output_id, value in outputs.items():
            CfnOutput(self, output_id, value=value)
