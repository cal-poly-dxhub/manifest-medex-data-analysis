import json
from typing import cast

from aws_cdk import App, Aspects, IAspect
from aws_cdk.assertions import Annotations, Match, Template
from cdk_nag import AwsSolutionsChecks
from src.config import AppConfig, DeploymentEnvironment
from src.stack import DataQualityStack


def build_stack() -> tuple[App, DataQualityStack, Template]:
    """Build the stack without AWS credentials for infrastructure assertions."""
    app = App()
    config = AppConfig(
        project_name="manifest-medex-data-quality",
        environment=DeploymentEnvironment.DEV,
    )
    stack = DataQualityStack(app, "TestStack", config=config)
    return app, stack, Template.from_stack(stack)


def test_stack_contains_the_secure_ingestion_foundation() -> None:
    app, _, template = build_stack()

    template.resource_count_is("AWS::KMS::Key", 1)
    template.resource_count_is("AWS::S3::Bucket", 4)
    template.resource_count_is("AWS::SQS::Queue", 4)
    template.resource_count_is("AWS::Events::Rule", 1)
    template.resource_count_is("AWS::CloudWatch::Alarm", 4)
    template.has_resource_properties("AWS::KMS::Key", {"EnableKeyRotation": True})

    template.has_resource_properties(
        "AWS::S3::Bucket",
        {"NotificationConfiguration": {"EventBridgeConfiguration": {"EventBridgeEnabled": True}}},
    )
    template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "EventPattern": {
                "detail-type": ["Object Created"],
                "source": ["aws.s3"],
            },
            "Targets": [
                {
                    "DeadLetterConfig": {"Arn": Match.any_value()},
                    "RetryPolicy": {
                        "MaximumEventAgeInSeconds": 7200,
                        "MaximumRetryAttempts": 10,
                    },
                }
            ],
        },
    )
    app.synth()


def test_buckets_are_private_encrypted_versioned_logged_and_retained() -> None:
    _, _, template = build_stack()
    buckets = template.find_resources("AWS::S3::Bucket")

    kms_encrypted_buckets = 0
    access_logged_buckets = 0
    for bucket in buckets.values():
        properties = bucket["Properties"]
        assert properties["PublicAccessBlockConfiguration"] == {
            "BlockPublicAcls": True,
            "BlockPublicPolicy": True,
            "IgnorePublicAcls": True,
            "RestrictPublicBuckets": True,
        }
        assert properties["VersioningConfiguration"] == {"Status": "Enabled"}
        assert bucket["DeletionPolicy"] == "Retain"
        assert bucket["UpdateReplacePolicy"] == "Retain"

        encryption = properties["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]
        algorithm = encryption["ServerSideEncryptionByDefault"]["SSEAlgorithm"]
        if algorithm == "aws:kms":
            kms_encrypted_buckets += 1
        if "LoggingConfiguration" in properties:
            access_logged_buckets += 1

    assert kms_encrypted_buckets == 3
    assert access_logged_buckets == 3
    bucket_policies = template.find_resources("AWS::S3::BucketPolicy")
    assert len(bucket_policies) == 4
    assert all("aws:SecureTransport" in json.dumps(policy) for policy in bucket_policies.values())
    assert all('"s3:TlsVersion": 1.2' in json.dumps(policy) for policy in bucket_policies.values())


def test_queues_are_kms_encrypted_tls_only_and_have_dead_letter_queues() -> None:
    _, _, template = build_stack()
    queues = template.find_resources("AWS::SQS::Queue")

    assert all("KmsMasterKeyId" in queue["Properties"] for queue in queues.values())
    assert all(
        queue["Properties"]["MessageRetentionPeriod"] == 1_209_600 for queue in queues.values()
    )
    assert sum("RedrivePolicy" in queue["Properties"] for queue in queues.values()) == 2
    assert (
        sum(queue["Properties"].get("VisibilityTimeout") == 900 for queue in queues.values()) == 1
    )
    assert (
        sum(queue["Properties"].get("VisibilityTimeout") == 300 for queue in queues.values()) == 1
    )

    queue_policies = template.find_resources("AWS::SQS::QueuePolicy")
    assert len(queue_policies) == 4
    assert all("aws:SecureTransport" in json.dumps(policy) for policy in queue_policies.values())


def test_cdk_nag_has_no_unsuppressed_aws_solutions_errors() -> None:
    app, stack, _ = build_stack()
    Aspects.of(app).add(cast(IAspect, AwsSolutionsChecks(verbose=True)))

    app.synth()
    errors = Annotations.from_stack(stack).find_error(
        "*",
        Match.string_like_regexp("AwsSolutions-.*"),
    )
    assert errors == []
