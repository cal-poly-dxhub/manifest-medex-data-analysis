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


def test_stack_contains_only_the_two_combined_ingestion_lanes() -> None:
    app, _, template = build_stack()

    template.resource_count_is("AWS::KMS::Key", 1)
    template.resource_count_is("AWS::S3::Bucket", 4)
    template.resource_count_is("AWS::SQS::Queue", 4)
    template.resource_count_is("AWS::Events::Rule", 2)
    template.resource_count_is("AWS::Lambda::Function", 2)
    template.resource_count_is("AWS::Lambda::EventSourceMapping", 2)
    template.resource_count_is("AWS::EC2::VPC", 1)
    template.resource_count_is("AWS::RDS::DBCluster", 1)
    template.resource_count_is("AWS::RDS::DBInstance", 1)
    template.resource_count_is("AWS::RDS::DBProxy", 0)
    template.resource_count_is("AWS::SecretsManager::Secret", 1)
    template.resource_count_is("AWS::OpenSearchServerless::Collection", 1)
    template.resource_count_is("AWS::OpenSearchServerless::VpcEndpoint", 1)
    template.resource_count_is("AWS::CloudWatch::Alarm", 6)
    template.resource_count_is("AWS::OpenSearchService::Domain", 0)

    handlers = {
        function["Properties"]["Handler"]
        for function in template.find_resources("AWS::Lambda::Function").values()
    }
    assert handlers == {"src.hl7_handler.handler", "src.ccda_handler.handler"}
    assert "src.parse_handler.handler" not in handlers
    assert "src.index_handler.handler" not in handlers

    assembly = app.synth()
    asset_roots = [
        path.parents[1] for path in Path(assembly.directory).glob("asset.*/hl7/__init__.py")
    ]
    assert len(asset_roots) == 1
    asset_root = asset_roots[0]
    for module in (
        "hl7_handler.py",
        "ccda_handler.py",
        "document_handler.py",
        "metadata_store.py",
        "search_store.py",
    ):
        assert (asset_root / "src" / module).is_file()
    assert (asset_root / "hl7-0.4.5.dist-info").is_dir()
    assert (asset_root / "defusedxml-0.7.1.dist-info").is_dir()
    assert not (asset_root / "tests").exists()
    assert not (asset_root / "tools").exists()


def test_eventbridge_routes_only_new_objects_under_format_prefixes() -> None:
    _, _, template = build_stack()
    rules = template.find_resources("AWS::Events::Rule")

    assert len(rules) == 2
    prefixes: set[str] = set()
    target_arns: set[str] = set()
    for rule in rules.values():
        properties = rule["Properties"]
        pattern = properties["EventPattern"]
        assert pattern["source"] == ["aws.s3"]
        assert pattern["detail-type"] == ["Object Created"]
        prefixes.add(pattern["detail"]["object"]["key"][0]["prefix"])
        target = properties["Targets"][0]
        target_arns.add(json.dumps(target["Arn"]))
        assert "DeadLetterConfig" in target
        assert target["RetryPolicy"] == {
            "MaximumEventAgeInSeconds": 7200,
            "MaximumRetryAttempts": 10,
        }
    assert prefixes == {"incoming/hl7/", "incoming/ccda/"}
    assert len(target_arns) == 2


def test_queues_are_separate_encrypted_tls_only_and_redrive_after_five_receives() -> None:
    _, _, template = build_stack()
    queues = template.find_resources("AWS::SQS::Queue")

    assert len(queues) == 4
    assert all("KmsMasterKeyId" in queue["Properties"] for queue in queues.values())
    assert all(
        queue["Properties"]["MessageRetentionPeriod"] == 1_209_600 for queue in queues.values()
    )
    processing = [queue for queue in queues.values() if "RedrivePolicy" in queue["Properties"]]
    assert len(processing) == 2
    assert all(queue["Properties"]["VisibilityTimeout"] == 900 for queue in processing)
    assert all(queue["Properties"]["RedrivePolicy"]["maxReceiveCount"] == 5 for queue in processing)

    queue_policies = template.find_resources("AWS::SQS::QueuePolicy")
    assert len(queue_policies) == 4
    assert all("aws:SecureTransport" in json.dumps(policy) for policy in queue_policies.values())


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
        if encryption["ServerSideEncryptionByDefault"]["SSEAlgorithm"] == "aws:kms":
            kms_encrypted_buckets += 1
        if "LoggingConfiguration" in properties:
            access_logged_buckets += 1
    assert kms_encrypted_buckets == 3
    assert access_logged_buckets == 3

    bucket_policies = template.find_resources("AWS::S3::BucketPolicy")
    assert len(bucket_policies) == 4
    assert all("aws:SecureTransport" in json.dumps(policy) for policy in bucket_policies.values())
    assert all('"s3:TlsVersion": 1.2' in json.dumps(policy) for policy in bucket_policies.values())


def test_format_lambdas_are_private_bounded_and_have_combined_dependencies() -> None:
    _, _, template = build_stack()
    functions = template.find_resources("AWS::Lambda::Function")

    assert len(functions) == 2
    assert all(function["Properties"]["Runtime"] == "python3.14" for function in functions.values())
    assert all(
        function["Properties"]["Architectures"] == ["arm64"] for function in functions.values()
    )
    assert all(function["Properties"]["Timeout"] == 600 for function in functions.values())
    assert all(function["Properties"]["MemorySize"] == 2048 for function in functions.values())
    assert all("VpcConfig" in function["Properties"] for function in functions.values())
    assert all("KmsKeyArn" in function["Properties"] for function in functions.values())
    assert sorted(
        function["Properties"]["ReservedConcurrentExecutions"] for function in functions.values()
    ) == [5, 10]

    formats: set[str] = set()
    for function in functions.values():
        environment = function["Properties"]["Environment"]["Variables"]
        formats.add(environment["SOURCE_FORMAT"])
        assert environment["METADATA_DATABASE"] == "manifest_medex"
        assert environment["METADATA_TABLE"] == "document_metadata"
        assert environment["OPENSEARCH_SERVICE"] == "aoss"
        assert environment["OPENSEARCH_HL7_INDEX"] == "hl7-messages-v1"
        assert environment["OPENSEARCH_CCDA_INDEX"] == "ccda-documents-v1"
        assert "METADATA_CLUSTER_ARN" in environment
        assert "METADATA_SECRET_ARN" in environment
        assert not any("PASSWORD" in key for key in environment)
    assert formats == {"hl7-v2", "ccda"}

    mappings = template.find_resources("AWS::Lambda::EventSourceMapping")
    assert len(mappings) == 2
    assert all(mapping["Properties"]["BatchSize"] == 1 for mapping in mappings.values())
    assert sorted(
        mapping["Properties"]["ScalingConfig"]["MaximumConcurrency"]
        for mapping in mappings.values()
    ) == [5, 10]
    assert all(
        mapping["Properties"]["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
        for mapping in mappings.values()
    )


def test_aurora_is_one_private_serverless_v2_writer_with_data_api_and_retention() -> None:
    _, _, template = build_stack()
    cluster = next(iter(template.find_resources("AWS::RDS::DBCluster").values()))
    properties = cluster["Properties"]

    assert properties["Engine"] == "aurora-postgresql"
    assert properties["EngineVersion"] == "16.6"
    assert properties["EnableHttpEndpoint"] is True
    assert properties["EnableIAMDatabaseAuthentication"] is True
    assert properties["DatabaseName"] == "manifest_medex"
    assert properties["ServerlessV2ScalingConfiguration"] == {
        "MinCapacity": 0.5,
        "MaxCapacity": 2,
    }
    assert properties["StorageEncrypted"] is True
    assert "KmsKeyId" in properties
    assert properties["BackupRetentionPeriod"] == 7
    assert properties["DeletionProtection"] is False
    assert cluster["DeletionPolicy"] == "Retain"
    assert cluster["UpdateReplacePolicy"] == "Retain"

    instances = template.find_resources("AWS::RDS::DBInstance")
    assert len(instances) == 1
    writer = next(iter(instances.values()))
    assert writer["Properties"]["DBInstanceClass"] == "db.serverless"
    assert writer["Properties"]["PubliclyAccessible"] is False
    assert writer["Properties"]["PromotionTier"] == 0

    secret = next(iter(template.find_resources("AWS::SecretsManager::Secret").values()))
    assert secret["Properties"]["GenerateSecretString"]["SecretStringTemplate"] == (
        '{"username":"metadata_admin"}'
    )
    assert "KmsKeyId" in secret["Properties"]
    assert secret["DeletionPolicy"] == "Retain"
    assert secret["UpdateReplacePolicy"] == "Retain"


def test_private_endpoints_allow_https_only_from_ingestion_security_group() -> None:
    _, _, template = build_stack()
    endpoints = template.find_resources("AWS::EC2::VPCEndpoint")
    assert len(endpoints) == 2
    interface_endpoint = next(
        endpoint
        for endpoint in endpoints.values()
        if endpoint["Properties"]["VpcEndpointType"] == "Interface"
    )
    endpoint_json = json.dumps(interface_endpoint)
    assert ".rds-data" in endpoint_json
    assert interface_endpoint["Properties"]["PrivateDnsEnabled"] is True
    assert len(interface_endpoint["Properties"]["SubnetIds"]) == 2
    assert "rds-data:ExecuteStatement" in endpoint_json
    assert "rds-data:BatchExecuteStatement" in endpoint_json
    assert "Hl7IngestionRole" in endpoint_json
    assert "CcdaIngestionRole" in endpoint_json

    ingress_rules = template.find_resources("AWS::EC2::SecurityGroupIngress")
    assert len(ingress_rules) == 2
    for ingress in ingress_rules.values():
        properties = ingress["Properties"]
        assert properties["IpProtocol"] == "tcp"
        assert properties["FromPort"] == 443
        assert properties["ToPort"] == 443
        assert "IngestionSecurityGroup" in json.dumps(properties["SourceSecurityGroupId"])
    assert not any("CidrIp" in ingress["Properties"] for ingress in ingress_rules.values())

    cluster_ingress = [
        ingress
        for ingress in ingress_rules.values()
        if ingress["Properties"].get("FromPort") == 5432
    ]
    assert cluster_ingress == []


def test_opensearch_remains_private_and_both_roles_have_exact_index_write_access() -> None:
    _, _, template = build_stack()
    collection = next(
        iter(template.find_resources("AWS::OpenSearchServerless::Collection").values())
    )
    assert collection["Properties"]["Name"] == "manifest-medex-dev"
    assert collection["Properties"]["DeletionProtection"] == "ENABLED"
    assert collection["Properties"]["StandbyReplicas"] == "DISABLED"
    assert collection["DeletionPolicy"] == "Retain"

    policies = template.find_resources("AWS::OpenSearchServerless::SecurityPolicy")
    network_policy = next(
        policy for policy in policies.values() if policy["Properties"]["Type"] == "network"
    )
    network_json = json.dumps(network_policy)
    assert "AllowFromPublic" in network_json
    assert "false" in network_json
    assert "SourceVPCEs" in network_json
    assert "true" not in network_json

    access_policy = next(
        iter(template.find_resources("AWS::OpenSearchServerless::AccessPolicy").values())
    )
    access_json = json.dumps(access_policy)
    assert "Hl7IngestionRole" in access_json
    assert "CcdaIngestionRole" in access_json
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


def test_iam_scopes_data_api_secret_s3_and_collection_access() -> None:
    _, _, template = build_stack()
    policies_json = json.dumps(template.find_resources("AWS::IAM::Policy"))

    assert "rds-data:ExecuteStatement" in policies_json
    assert "rds-data:BatchExecuteStatement" in policies_json
    assert "secretsmanager:GetSecretValue" in policies_json
    assert "aoss:APIAccessAll" in policies_json
    assert "incoming/hl7/*" in policies_json
    assert "incoming/ccda/*" in policies_json
    assert "parsed/" not in policies_json
    assert "s3:DeleteObject" not in policies_json
    assert "rds-data:BeginTransaction" not in policies_json
    assert "rds-data:CommitTransaction" not in policies_json
    assert "aoss:DashboardsAccessAll" not in policies_json
    assert "es:ESHttp" not in policies_json


def test_kms_key_allows_logs_for_only_three_stack_log_groups() -> None:
    _, _, template = build_stack()
    key = next(iter(template.find_resources("AWS::KMS::Key").values()))
    assert key["Properties"]["EnableKeyRotation"] is True
    statements = key["Properties"]["KeyPolicy"]["Statement"]
    logs_statement = next(
        statement
        for statement in statements
        if statement.get("Sid") == "AllowCloudWatchLogsForStackLogGroups"
    )
    contexts = logs_statement["Condition"]["ArnEquals"]["kms:EncryptionContext:aws:logs:arn"]
    assert len(contexts) == 3
    context_json = json.dumps(contexts)
    assert "/aws/vpc/manifest-medex-data-quality-dev" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-hl7" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-ccda" in context_json
    assert "parser" not in context_json
    assert "indexer" not in context_json


def test_development_dashboard_opt_in_is_public_and_read_only_for_one_role() -> None:
    role_arn = "arn:aws:iam::111122223333:role/DevelopmentDashboardRole"
    _, _, template = build_stack(
        enable_public_dashboard=True,
        dashboard_principal_arn=role_arn,
    )
    policies = template.find_resources("AWS::OpenSearchServerless::SecurityPolicy")
    network_policy = next(
        policy for policy in policies.values() if policy["Properties"]["Type"] == "network"
    )
    network_json = json.dumps(network_policy)
    assert "Temporary public development Dashboard browser access" in network_json
    assert "true" in network_json

    access_json = json.dumps(
        next(iter(template.find_resources("AWS::OpenSearchServerless::AccessPolicy").values()))
    )
    assert role_arn in access_json
    assert "aoss:ReadDocument" in access_json
    assert "aoss:DeleteDocument" not in access_json
    assert "aoss:*" not in access_json


def test_production_enables_aurora_and_collection_protection() -> None:
    _, _, template = build_stack(DeploymentEnvironment.PROD)
    cluster = next(iter(template.find_resources("AWS::RDS::DBCluster").values()))
    assert cluster["Properties"]["DeletionProtection"] is True
    assert cluster["Properties"]["BackupRetentionPeriod"] == 35
    collection = next(
        iter(template.find_resources("AWS::OpenSearchServerless::Collection").values())
    )
    assert collection["Properties"]["StandbyReplicas"] == "ENABLED"


def test_stack_outputs_both_lanes_and_aurora_metadata_locations() -> None:
    _, _, template = build_stack()
    outputs = template.to_json()["Outputs"]
    for output in (
        "Hl7QueueUrl",
        "CcdaQueueUrl",
        "Hl7FunctionName",
        "CcdaFunctionName",
        "AuroraClusterArn",
        "AuroraClusterIdentifier",
        "AuroraSecretArn",
        "MetadataDatabaseName",
        "MetadataTableName",
    ):
        assert output in outputs
    assert outputs["MetadataTableName"]["Value"] == "document_metadata"


def test_cdk_nag_has_no_unsuppressed_aws_solutions_errors() -> None:
    app, stack, _ = build_stack()
    Aspects.of(app).add(cast(IAspect, AwsSolutionsChecks(verbose=True)))

    app.synth()
    errors = Annotations.from_stack(stack).find_error(
        "*",
        Match.string_like_regexp("AwsSolutions-.*"),
    )
    assert errors == []
