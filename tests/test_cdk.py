import json
from pathlib import Path
from typing import cast

from aws_cdk import App, Aspects, IAspect
from aws_cdk.assertions import Annotations, Match, Template
from cdk_nag import AwsSolutionsChecks
from src.config import AppConfig, DeploymentEnvironment
from src.stack import DataQualityStack


def build_stack(
    environment: DeploymentEnvironment = DeploymentEnvironment.DEV,
    *,
    enable_public_dashboard: bool = False,
    dashboard_principal_arn: str | None = None,
) -> tuple[App, DataQualityStack, Template]:
    """Build the stack without AWS credentials for infrastructure assertions."""
    app = App()
    config = AppConfig(
        project_name="manifest-medex-data-quality",
        environment=environment,
        enable_public_dashboard=enable_public_dashboard,
        dashboard_principal_arn=dashboard_principal_arn,
        termination_protection=environment is DeploymentEnvironment.PROD,
    )
    stack = DataQualityStack(app, "TestStack", config=config)
    return app, stack, Template.from_stack(stack)


def test_stack_contains_the_secure_ingestion_foundation() -> None:
    app, _, template = build_stack()

    template.resource_count_is("AWS::KMS::Key", 1)
    template.resource_count_is("AWS::S3::Bucket", 4)
    template.resource_count_is("AWS::SQS::Queue", 4)
    template.resource_count_is("AWS::Events::Rule", 1)
    template.resource_count_is("AWS::Lambda::Function", 2)
    template.resource_count_is("AWS::Lambda::EventSourceMapping", 2)
    template.resource_count_is("AWS::EC2::VPC", 1)
    template.resource_count_is("AWS::OpenSearchServerless::Collection", 1)
    template.resource_count_is("AWS::OpenSearchServerless::SecurityPolicy", 2)
    template.resource_count_is("AWS::OpenSearchServerless::AccessPolicy", 1)
    template.resource_count_is("AWS::OpenSearchServerless::VpcEndpoint", 1)
    template.resource_count_is("AWS::OpenSearchService::Domain", 0)
    template.resource_count_is("AWS::CloudWatch::Alarm", 6)
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
    assembly = app.synth()
    asset_roots = [
        path.parents[1] for path in Path(assembly.directory).glob("asset.*/hl7/__init__.py")
    ]
    assert len(asset_roots) == 1
    asset_root = asset_roots[0]
    assert (asset_root / "src" / "parse_handler.py").is_file()
    assert (asset_root / "hl7-0.4.5.dist-info").is_dir()
    assert (asset_root / "defusedxml" / "ElementTree.py").is_file()
    assert (asset_root / "defusedxml-0.7.1.dist-info").is_dir()
    assert not (asset_root / "tests").exists()


def test_kms_key_allows_cloudwatch_logs_for_only_stack_log_groups() -> None:
    _, _, template = build_stack()
    key = next(iter(template.find_resources("AWS::KMS::Key").values()))
    statements = key["Properties"]["KeyPolicy"]["Statement"]
    logs_statement = next(
        statement
        for statement in statements
        if "logs." in json.dumps(statement.get("Principal", {}))
    )

    assert logs_statement["Sid"] == "AllowCloudWatchLogsForStackLogGroups"
    assert logs_statement["Effect"] == "Allow"
    assert set(logs_statement["Action"]) == {
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:Describe*",
    }
    assert logs_statement["Resource"] == "*"

    principal_json = json.dumps(logs_statement["Principal"])
    assert "logs." in principal_json
    assert "AWS::Region" in principal_json
    assert "AWS::URLSuffix" in principal_json

    encryption_context = logs_statement["Condition"]["ArnEquals"][
        "kms:EncryptionContext:aws:logs:arn"
    ]
    assert len(encryption_context) == 3
    context_json = json.dumps(encryption_context)
    assert "/aws/vpc/manifest-medex-data-quality-dev" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-parser" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-indexer" in context_json


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
        sum(queue["Properties"].get("VisibilityTimeout") == 360 for queue in queues.values()) == 1
    )

    queue_policies = template.find_resources("AWS::SQS::QueuePolicy")
    assert len(queue_policies) == 4
    assert all("aws:SecureTransport" in json.dumps(policy) for policy in queue_policies.values())


def test_compute_and_search_are_private_encrypted_and_operationally_bounded() -> None:
    _, _, template = build_stack()

    functions = template.find_resources("AWS::Lambda::Function")
    assert len(functions) == 2
    assert all(function["Properties"]["Runtime"] == "python3.14" for function in functions.values())
    assert all(
        function["Properties"]["Architectures"] == ["arm64"] for function in functions.values()
    )
    assert all("KmsKeyArn" in function["Properties"] for function in functions.values())
    assert all(
        function["Properties"]["TracingConfig"] == {"Mode": "Active"}
        for function in functions.values()
    )
    assert sorted(
        function["Properties"]["ReservedConcurrentExecutions"] for function in functions.values()
    ) == [5, 10]

    mappings = template.find_resources("AWS::Lambda::EventSourceMapping")
    assert len(mappings) == 2
    assert sorted(mapping["Properties"]["BatchSize"] for mapping in mappings.values()) == [1, 10]
    assert all(
        mapping["Properties"]["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
        for mapping in mappings.values()
    )
    indexer = next(
        function
        for function in functions.values()
        if function["Properties"]["Handler"] == "src.index_handler.handler"
    )
    indexer_environment = indexer["Properties"]["Environment"]["Variables"]
    assert indexer_environment["OPENSEARCH_SERVICE"] == "aoss"
    assert indexer_environment["OPENSEARCH_ENDPOINT"] == {
        "Fn::GetAtt": ["SearchCollection", "CollectionEndpoint"]
    }
    assert indexer_environment["OPENSEARCH_HL7_INDEX"] == "hl7-messages-v1"
    assert indexer_environment["OPENSEARCH_CCDA_INDEX"] == "ccda-documents-v1"

    collections = template.find_resources("AWS::OpenSearchServerless::Collection")
    assert len(collections) == 1
    collection = next(iter(collections.values()))
    collection_properties = collection["Properties"]
    assert collection_properties["Name"] == "manifest-medex-dev"
    assert collection_properties["Type"] == "SEARCH"
    assert collection_properties["DeletionProtection"] == "ENABLED"
    assert collection_properties["StandbyReplicas"] == "DISABLED"
    assert "CollectionGroupName" not in collection["Properties"]
    assert collection["DeletionPolicy"] == "Retain"
    assert collection["UpdateReplacePolicy"] == "Retain"

    security_policies = template.find_resources("AWS::OpenSearchServerless::SecurityPolicy")
    assert len(security_policies) == 2
    encryption_policy = next(
        policy
        for policy in security_policies.values()
        if policy["Properties"]["Type"] == "encryption"
    )
    encryption_json = json.dumps(encryption_policy)
    assert "AWSOwnedKey" in encryption_json
    assert "false" in encryption_json
    assert "EncryptionKey" in encryption_json
    assert "collection/manifest-medex-dev" in encryption_json

    network_policy = next(
        policy for policy in security_policies.values() if policy["Properties"]["Type"] == "network"
    )
    network_json = json.dumps(network_policy)
    assert "AllowFromPublic" in network_json
    assert "false" in network_json
    assert "SourceVPCEs" in network_json
    assert "ServerlessVpcEndpoint" in network_json
    assert "ResourceType" in network_json
    assert "collection" in network_json
    assert "dashboard" in network_json
    assert "true" not in network_json
    assert "Temporary public development Dashboard browser access" not in network_json

    endpoints = template.find_resources("AWS::OpenSearchServerless::VpcEndpoint")
    endpoint = next(iter(endpoints.values()))
    assert endpoint["Properties"]["VpcId"] == {"Ref": "SearchVpc327F9A34"}
    assert len(endpoint["Properties"]["SubnetIds"]) == 2
    assert len(endpoint["Properties"]["SecurityGroupIds"]) == 1
    ingress = next(iter(template.find_resources("AWS::EC2::SecurityGroupIngress").values()))
    assert ingress["Properties"]["IpProtocol"] == "tcp"
    assert ingress["Properties"]["FromPort"] == 443
    assert ingress["Properties"]["ToPort"] == 443
    assert "IndexerSecurityGroup" in json.dumps(ingress["Properties"]["SourceSecurityGroupId"])
    assert "ServerlessEndpointSecurityGroup" in json.dumps(ingress["Properties"]["GroupId"])

    access_policies = template.find_resources("AWS::OpenSearchServerless::AccessPolicy")
    access_json = json.dumps(next(iter(access_policies.values())))
    assert "index/manifest-medex-dev/hl7-messages-v1" in access_json
    assert "index/manifest-medex-dev/ccda-documents-v1" in access_json
    for permission in (
        "aoss:CreateIndex",
        "aoss:DescribeIndex",
        "aoss:UpdateIndex",
        "aoss:WriteDocument",
    ):
        assert permission in access_json
    assert "aoss:*" not in access_json

    iam_json = json.dumps(template.find_resources("AWS::IAM::Policy"))
    assert "aoss:APIAccessAll" in iam_json
    assert "aoss:DashboardsAccessAll" not in iam_json
    assert "es:ESHttp" not in iam_json
    assert not template.find_resources("AWS::OpenSearchService::Domain")

    resources = template.to_json()["Resources"].values()
    assert not any(str(resource["Type"]).startswith("Custom::") for resource in resources)


def test_development_dashboard_opt_in_is_public_and_read_only_for_one_role() -> None:
    role_arn = "arn:aws:iam::111122223333:role/DevelopmentDashboardRole"
    _, _, template = build_stack(
        enable_public_dashboard=True,
        dashboard_principal_arn=role_arn,
    )

    security_policies = template.find_resources("AWS::OpenSearchServerless::SecurityPolicy")
    network_policy = next(
        policy for policy in security_policies.values() if policy["Properties"]["Type"] == "network"
    )
    network_json = json.dumps(network_policy)
    assert "Private API access from the dedicated VPC endpoint" in network_json
    assert "Temporary public development Dashboard browser access" in network_json
    assert "SourceVPCEs" in network_json
    assert "false" in network_json
    assert "true" in network_json

    access_policy = next(
        iter(template.find_resources("AWS::OpenSearchServerless::AccessPolicy").values())
    )
    access_json = json.dumps(access_policy)
    assert role_arn in access_json
    assert "Temporary read-only development Dashboard access" in access_json
    assert "aoss:DescribeIndex" in access_json
    assert "aoss:ReadDocument" in access_json
    assert "aoss:DeleteIndex" not in access_json
    assert "aoss:DeleteDocument" not in access_json
    assert "aoss:*" not in access_json


def test_production_collection_uses_classic_model_with_standby_replicas() -> None:
    _, _, template = build_stack(DeploymentEnvironment.PROD)

    collection = next(
        iter(template.find_resources("AWS::OpenSearchServerless::Collection").values())
    )
    assert collection["Properties"]["StandbyReplicas"] == "ENABLED"
    assert "CollectionGroupName" not in collection["Properties"]


def test_cdk_nag_has_no_unsuppressed_aws_solutions_errors() -> None:
    app, stack, _ = build_stack()
    Aspects.of(app).add(cast(IAspect, AwsSolutionsChecks(verbose=True)))

    app.synth()
    errors = Annotations.from_stack(stack).find_error(
        "*",
        Match.string_like_regexp("AwsSolutions-.*"),
    )
    assert errors == []
