import json
import re
from pathlib import Path
from typing import Any, cast

import pytest
from aws_cdk import App, Aspects, IAspect
from aws_cdk.assertions import Template
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
    template.resource_count_is("AWS::S3::Bucket", 6)
    template.resource_count_is("AWS::SQS::Queue", 6)
    template.resource_count_is("AWS::Events::Rule", 2)
    template.resource_count_is("AWS::Lambda::Function", 7)
    template.resource_count_is("AWS::Lambda::EventSourceMapping", 3)
    template.resource_count_is("AWS::EC2::VPC", 1)
    template.resource_count_is("AWS::DynamoDB::Table", 3)
    template.resource_count_is("AWS::RDS::DBCluster", 1)
    template.resource_count_is("AWS::RDS::DBInstance", 1)
    template.resource_count_is("AWS::RDS::DBProxy", 0)
    template.resource_count_is("AWS::SecretsManager::Secret", 1)
    template.resource_count_is("AWS::OpenSearchServerless::Collection", 1)
    template.resource_count_is("AWS::OpenSearchServerless::VpcEndpoint", 1)
    template.resource_count_is("AWS::CloudWatch::Alarm", 8)
    template.resource_count_is("AWS::OpenSearchService::Domain", 0)

    handlers = {
        function["Properties"]["Handler"]
        for function in template.find_resources("AWS::Lambda::Function").values()
    }
    assert "src.hl7_handler.handler" in handlers
    assert "src.ccda_handler.handler" in handlers
    assert "src.explorer_handler.handler" in handlers
    assert "src.report_runner.handler" in handlers
    assert "src.reingest_planner.handler" in handlers
    assert "src.reindexer_handler.handler" in handlers
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
        "customer_field_catalog.py",
        "document_handler.py",
        "metadata_store.py",
        "search_store.py",
        "report_runner.py",
        "report_catalog.py",
        "report_runs.py",
        "report_definition.py",
        "report_facilities.py",
        "report_csv.py",
        "report_query.py",
        "message_search.py",
        "reingest_jobs.py",
        "reingest_planner.py",
        "reindexer_handler.py",
    ):
        assert (asset_root / "src" / module).is_file()
    # report_definition loads its JSON Schema from the bundled schema/ directory.
    assert (asset_root / "schema" / "report_definition.schema.json").is_file()
    assert (asset_root / "hl7-0.4.5.dist-info").is_dir()
    assert (asset_root / "defusedxml-0.7.1.dist-info").is_dir()
    assert not (asset_root / "src" / "stack.py").exists()
    assert not (asset_root / "src" / "config.py").exists()
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

    assert len(queues) == 6
    assert all("KmsMasterKeyId" in queue["Properties"] for queue in queues.values())
    assert all(
        queue["Properties"]["MessageRetentionPeriod"] == 1_209_600 for queue in queues.values()
    )
    processing = [queue for queue in queues.values() if "RedrivePolicy" in queue["Properties"]]
    assert len(processing) == 3
    assert all(queue["Properties"]["VisibilityTimeout"] == 900 for queue in processing)
    assert all(queue["Properties"]["RedrivePolicy"]["maxReceiveCount"] == 5 for queue in processing)

    queue_policies = template.find_resources("AWS::SQS::QueuePolicy")
    assert len(queue_policies) == 6
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
    assert kms_encrypted_buckets == 4
    assert access_logged_buckets == 5

    bucket_policies = template.find_resources("AWS::S3::BucketPolicy")
    assert len(bucket_policies) == 6
    assert all("aws:SecureTransport" in json.dumps(policy) for policy in bucket_policies.values())
    assert all('"s3:TlsVersion": 1.2' in json.dumps(policy) for policy in bucket_policies.values())


def test_format_lambdas_are_private_bounded_and_have_combined_dependencies() -> None:
    _, _, template = build_stack()
    ingestion_handlers = {"src.hl7_handler.handler", "src.ccda_handler.handler"}
    functions = {
        logical_id: function
        for logical_id, function in template.find_resources("AWS::Lambda::Function").items()
        if function["Properties"]["Handler"] in ingestion_handlers
    }

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

    mappings = {
        logical_id: mapping
        for logical_id, mapping in template.find_resources(
            "AWS::Lambda::EventSourceMapping"
        ).items()
        if any(
            name in json.dumps(mapping["Properties"].get("EventSourceArn", {}))
            for name in ("Hl7Queue", "CcdaQueue")
        )
    }
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
    assert properties["EngineVersion"] == "16.8"
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
    # S3 and DynamoDB gateway endpoints plus the RDS Data API, Lambda, and SQS interface
    # endpoints. SQS is required because the reingestion planner sends to the reindex
    # queue from the isolated subnets (every other function is queue-invoked).
    assert len(endpoints) == 5
    interface_endpoints = [
        endpoint
        for endpoint in endpoints.values()
        if endpoint["Properties"]["VpcEndpointType"] == "Interface"
    ]
    assert len(interface_endpoints) == 3
    interface_endpoint = next(
        endpoint for endpoint in interface_endpoints if ".rds-data" in json.dumps(endpoint)
    )
    endpoint_json = json.dumps(interface_endpoint)
    assert ".rds-data" in endpoint_json
    assert interface_endpoint["Properties"]["PrivateDnsEnabled"] is True
    assert len(interface_endpoint["Properties"]["SubnetIds"]) == 2
    assert "rds-data:ExecuteStatement" in endpoint_json
    assert "rds-data:BatchExecuteStatement" in endpoint_json
    assert "Hl7IngestionRole" in endpoint_json
    assert "CcdaIngestionRole" in endpoint_json
    assert "ExplorerRole" in endpoint_json

    lambda_endpoint = next(
        endpoint for endpoint in interface_endpoints if ".lambda" in json.dumps(endpoint)
    )
    assert lambda_endpoint["Properties"]["PrivateDnsEnabled"] is True

    sqs_endpoint = next(
        endpoint for endpoint in interface_endpoints if ".sqs" in json.dumps(endpoint)
    )
    sqs_json = json.dumps(sqs_endpoint)
    assert sqs_endpoint["Properties"]["PrivateDnsEnabled"] is True
    assert len(sqs_endpoint["Properties"]["SubnetIds"]) == 2
    # Least privilege: only the planner may send, and only to the reindex queue.
    assert "sqs:SendMessage" in sqs_json
    assert "ReingestPlannerRole" in sqs_json
    assert "ReindexQueue" in sqs_json
    assert "Hl7IngestionRole" not in sqs_json
    assert "sqs:ReceiveMessage" not in sqs_json

    ingress_rules = template.find_resources("AWS::EC2::SecurityGroupIngress")
    assert len(ingress_rules) == 9
    # The SQS endpoint admits only the reingestion planner; ingestion Lambdas are
    # queue-invoked and must not gain an outbound SQS path they do not need.
    sqs_ingress = [
        ingress
        for ingress in ingress_rules.values()
        if "SqsEndpointSecurityGroup" in json.dumps(ingress["Properties"]["GroupId"])
    ]
    assert len(sqs_ingress) == 1
    assert "ReingestPlannerSecurityGroup" in json.dumps(
        sqs_ingress[0]["Properties"]["SourceSecurityGroupId"]
    )
    allowed_sources = {
        "IngestionSecurityGroup",
        "ExplorerSecurityGroup",
        "ReportRunnerSecurityGroup",
        "ReingestPlannerSecurityGroup",
        "ReindexerSecurityGroup",
    }
    for ingress in ingress_rules.values():
        properties = ingress["Properties"]
        assert properties["IpProtocol"] == "tcp"
        assert properties["FromPort"] == 443
        assert properties["ToPort"] == 443
        source = json.dumps(properties["SourceSecurityGroupId"])
        assert any(name in source for name in allowed_sources)
    assert not any("CidrIp" in ingress["Properties"] for ingress in ingress_rules.values())
    explorer_ingress = [
        ingress
        for ingress in ingress_rules.values()
        if "ExplorerSecurityGroup" in json.dumps(ingress["Properties"]["SourceSecurityGroupId"])
    ]
    # Explorer reaches the RDS Data API, OpenSearch Serverless, and the Lambda API.
    assert len(explorer_ingress) == 3
    runner_ingress = [
        ingress
        for ingress in ingress_rules.values()
        if "ReportRunnerSecurityGroup" in json.dumps(ingress["Properties"]["SourceSecurityGroupId"])
    ]
    # The runner reaches only OpenSearch Serverless over the network.
    assert len(runner_ingress) == 1

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
    # Exclude the CDK-managed BucketDeployment handler policy, which legitimately needs
    # delete/list permissions on the frontend bucket it prunes.
    stack_policies = {
        logical_id: policy
        for logical_id, policy in template.find_resources("AWS::IAM::Policy").items()
        if "BucketDeployment" not in json.dumps(policy["Properties"].get("Roles", []))
    }
    policies_json = json.dumps(stack_policies)

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


def test_kms_key_allows_logs_for_only_stack_log_groups() -> None:
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
    assert len(contexts) == 7
    context_json = json.dumps(contexts)
    assert "/aws/vpc/manifest-medex-data-quality-dev" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-hl7" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-ccda" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-explorer" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-report-runner" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-reingest-planner" in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-reindexer" in context_json
    assert "parser" not in context_json
    assert "/aws/lambda/manifest-medex-data-quality-dev-indexer" not in context_json


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


def _aws_solutions_errors(app: App, artifact_id: str) -> list[str]:
    """Collect unsuppressed AwsSolutions findings from the synthesized assembly.

    The assertions ``Annotations`` reader cannot deserialize the non-string metadata that
    aws-cdk-lib attaches to the CDK-managed BucketDeployment handler singleton, so the
    findings are read directly from the synthesized stack metadata instead.
    """
    assembly = app.synth()
    metadata_path = Path(assembly.directory) / f"{artifact_id}.metadata.json"
    metadata = json.loads(metadata_path.read_text())
    findings: list[str] = []
    pattern = re.compile(r"AwsSolutions-[A-Za-z0-9]+")
    for entries in metadata.values():
        for entry in entries:
            if entry.get("type") != "aws:cdk:error":
                continue
            data = entry.get("data")
            if isinstance(data, str) and pattern.match(data):
                findings.append(data)
    return findings


def test_cdk_nag_has_no_unsuppressed_aws_solutions_errors() -> None:
    app, stack, _ = build_stack()
    Aspects.of(app).add(cast(IAspect, AwsSolutionsChecks(verbose=True)))

    assert _aws_solutions_errors(app, stack.artifact_id) == []


def test_cdk_nag_detector_reports_findings_on_a_deliberately_unsafe_stack() -> None:
    # Guard against a silently-broken detector (e.g. wrong file path returning empty): a
    # deliberately non-compliant bucket must surface AwsSolutions findings through the scan.
    from aws_cdk import Stack
    from aws_cdk import aws_s3 as s3

    app = App()
    probe = Stack(app, "NagProbeStack")
    s3.CfnBucket(probe, "UnsafeBucket")
    Aspects.of(app).add(cast(IAspect, AwsSolutionsChecks(verbose=True)))

    findings = _aws_solutions_errors(app, probe.artifact_id)
    assert findings
    assert all(finding.startswith("AwsSolutions-") for finding in findings)


def _explorer_function(template: Template) -> dict[str, Any]:
    return next(
        cast(dict[str, Any], function)
        for function in template.find_resources("AWS::Lambda::Function").values()
        if function["Properties"]["Handler"] == "src.explorer_handler.handler"
    )


def test_explorer_lambda_is_private_python314_arm64_with_scoped_environment() -> None:
    _, _, template = build_stack()
    function = _explorer_function(template)["Properties"]

    assert function["Runtime"] == "python3.14"
    assert function["Architectures"] == ["arm64"]
    assert function["Handler"] == "src.explorer_handler.handler"
    assert function["Timeout"] == 30
    assert function["MemorySize"] == 1024
    assert "VpcConfig" in function
    assert "KmsKeyArn" in function

    environment = function["Environment"]["Variables"]
    assert environment["METADATA_DATABASE"] == "manifest_medex"
    assert environment["METADATA_TABLE"] == "document_metadata"
    assert "RAW_BUCKET" in environment
    assert "PARSED_BUCKET" in environment
    assert "METADATA_CLUSTER_ARN" in environment
    assert "METADATA_SECRET_ARN" in environment
    assert not any("PASSWORD" in key for key in environment)
    # The explorer also backs the Reports API and the facility lookup.
    assert "REPORT_BUCKET" in environment
    assert "REPORT_CATALOG_TABLE" in environment
    assert "REPORT_RUNS_TABLE" in environment
    assert environment["REPORT_RUNS_INDEX"] == "reportId-startedAt-index"
    assert "REPORT_RUNNER_FUNCTION" in environment
    assert environment["OPENSEARCH_ENDPOINT"]
    assert environment["OPENSEARCH_HL7_INDEX"] == "hl7-messages-v1"
    assert environment["OPENSEARCH_CCDA_INDEX"] == "ccda-documents-v1"


def test_explorer_role_has_execute_statement_and_read_only_storage_access() -> None:
    _, _, template = build_stack()
    roles = template.find_resources("AWS::IAM::Role")
    explorer_role_id = next(
        logical_id
        for logical_id, role in roles.items()
        if "explorer" in json.dumps(role.get("Properties", {}).get("Description", "")).lower()
    )
    policies = template.find_resources("AWS::IAM::Policy")
    explorer_policy = next(
        policy
        for policy in policies.values()
        if any(
            explorer_role_id in json.dumps(role_ref)
            for role_ref in policy["Properties"].get("Roles", [])
        )
    )
    statements = explorer_policy["Properties"]["PolicyDocument"]["Statement"]
    actions: set[str] = set()
    for statement in statements:
        action = statement["Action"]
        actions.update(action if isinstance(action, list) else [action])

    assert "s3:GetObject" in actions
    assert "s3:GetObjectVersion" in actions
    # Definitions now live in the catalog table, so the explorer writes and deletes no S3.
    assert "s3:PutObject" not in actions
    assert "s3:DeleteObject" not in actions
    assert "rds-data:ExecuteStatement" in actions
    assert "rds-data:BatchExecuteStatement" not in actions
    assert "secretsmanager:GetSecretValue" in actions
    assert "kms:Decrypt" in actions
    # Reports DynamoDB CMK usage legitimately requires encrypt as well as decrypt.
    assert "kms:Encrypt" in actions
    # The Reports API owns row-granular catalog CRUD, run bookkeeping, and worker dispatch.
    assert "dynamodb:GetItem" in actions
    assert "dynamodb:PutItem" in actions
    assert "dynamodb:UpdateItem" in actions
    assert "dynamodb:DeleteItem" in actions
    assert "dynamodb:BatchWriteItem" in actions
    # Each catalog mutation pairs its change with an append-only audit item transactionally.
    assert "dynamodb:TransactWriteItems" in actions
    assert "dynamodb:Query" in actions
    assert "dynamodb:Scan" in actions
    assert "lambda:InvokeFunction" in actions
    assert "aoss:APIAccessAll" in actions
    assert "dynamodb:DeleteTable" not in actions

    policy_json = json.dumps(explorer_policy)
    # Definitions are no longer stored in S3; only generated outputs are readable for download.
    assert "definitions/*" not in policy_json
    assert "outputs/*" in policy_json


def test_explorer_data_api_endpoint_policy_grants_execute_only() -> None:
    _, _, template = build_stack()
    endpoints = template.find_resources("AWS::EC2::VPCEndpoint")
    interface_endpoint = next(
        endpoint
        for endpoint in endpoints.values()
        if endpoint["Properties"]["VpcEndpointType"] == "Interface"
    )
    document = interface_endpoint["Properties"]["PolicyDocument"]
    explorer_statement = next(
        statement
        for statement in document["Statement"]
        if "ExplorerRole" in json.dumps(statement.get("Principal", {}))
    )
    action = explorer_statement["Action"]
    actions = set(action if isinstance(action, list) else [action])
    assert actions == {"rds-data:ExecuteStatement"}


def test_http_api_has_named_auto_deploy_stage_and_no_default_stage() -> None:
    _, _, template = build_stack()
    template.resource_count_is("AWS::ApiGatewayV2::Api", 1)
    stages = template.find_resources("AWS::ApiGatewayV2::Stage")
    assert len(stages) == 1
    stage = next(iter(stages.values()))["Properties"]
    assert stage["StageName"] == "api"
    assert stage["AutoDeploy"] is True
    # No access log settings so clinical document identifiers never reach request logs.
    assert "AccessLogSettings" not in stage


def test_http_api_exposes_only_the_authenticated_explorer_routes() -> None:
    _, _, template = build_stack()
    routes = template.find_resources("AWS::ApiGatewayV2::Route")
    route_keys = {route["Properties"]["RouteKey"] for route in routes.values()}
    assert route_keys == {
        "GET /messages",
        "GET /messages/{documentId}",
        "POST /messages/{documentId}/body",
        "POST /query",
        "POST /query-test",
        "POST /search",
        "GET /search/fields",
        "GET /facilities",
        "GET /reports",
        "POST /reports/import",
        "GET /reports/{id}",
        "PUT /reports/{id}",
        "DELETE /reports/{id}",
        "GET /reports/{id}/history",
        "GET /reports/{id}/export",
        "POST /reports/{id}/sections",
        "POST /reports/{id}/sections/{sseq}/rows",
        "PUT /reports/{id}/sections/{sseq}/rows/{rseq}",
        "DELETE /reports/{id}/sections/{sseq}/rows/{rseq}",
        "GET /reports/{id}/runs",
        "POST /reports/{id}/runs",
        "GET /runs/{runId}",
        "GET /runs/{runId}/download",
        "POST /reingest/preview",
        "POST /reingest/jobs",
        "GET /reingest/jobs",
        "GET /reingest/jobs/{jobId}",
    }
    assert all(route["Properties"]["AuthorizationType"] == "JWT" for route in routes.values())

    authorizers = template.find_resources("AWS::ApiGatewayV2::Authorizer")
    assert len(authorizers) == 1
    authorizer = next(iter(authorizers.values()))["Properties"]
    assert authorizer["AuthorizerType"] == "JWT"
    assert authorizer["IdentitySource"] == ["$request.header.Authorization"]

    authorizer_id = next(iter(authorizers.keys()))
    assert all(
        authorizer_id in json.dumps(route["Properties"].get("AuthorizerId"))
        for route in routes.values()
    )


def test_cognito_user_pool_enforces_no_self_signup_email_and_strong_policy() -> None:
    _, _, template = build_stack()
    user_pool = next(iter(template.find_resources("AWS::Cognito::UserPool").values()))["Properties"]

    assert user_pool["AdminCreateUserConfig"]["AllowAdminCreateUserOnly"] is True
    assert user_pool["UsernameAttributes"] == ["email"]
    password_policy = user_pool["Policies"]["PasswordPolicy"]
    assert password_policy["MinimumLength"] == 14
    assert password_policy["RequireLowercase"] is True
    assert password_policy["RequireUppercase"] is True
    assert password_policy["RequireNumbers"] is True
    assert password_policy["RequireSymbols"] is True
    assert user_pool["MfaConfiguration"] == "OPTIONAL"
    assert user_pool["EnabledMfas"] == ["SOFTWARE_TOKEN_MFA"]
    assert user_pool["UserPoolAddOns"]["AdvancedSecurityMode"] == "ENFORCED"

    template.resource_count_is("AWS::Cognito::UserPoolDomain", 1)


def test_cognito_app_client_is_public_auth_code_only_with_cloudfront_callbacks() -> None:
    _, _, template = build_stack()
    client = next(iter(template.find_resources("AWS::Cognito::UserPoolClient").values()))[
        "Properties"
    ]

    assert client["GenerateSecret"] is False
    assert client["AllowedOAuthFlows"] == ["code"]
    assert client["AllowedOAuthFlowsUserPoolClient"] is True
    assert client["SupportedIdentityProviders"] == ["COGNITO"]
    assert sorted(client["AllowedOAuthScopes"]) == ["email", "openid", "profile"]
    callbacks = json.dumps(client["CallbackURLs"])
    logouts = json.dumps(client["LogoutURLs"])
    assert "FrontendDistribution" in callbacks
    assert "FrontendDistribution" in logouts
    # The public client never receives a generated secret in Secrets Manager.
    template.resource_count_is("AWS::SecretsManager::Secret", 1)


def test_frontend_bucket_is_private_encrypted_ssl_and_access_logged() -> None:
    _, _, template = build_stack()
    buckets = template.find_resources("AWS::S3::Bucket")
    frontend = next(
        bucket
        for bucket in buckets.values()
        if bucket["Properties"].get("LoggingConfiguration", {}).get("LogFilePrefix") == "frontend/"
    )
    properties = frontend["Properties"]
    assert properties["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    encryption = properties["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]
    assert encryption["ServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    assert frontend["DeletionPolicy"] == "Retain"

    bucket_policies = template.find_resources("AWS::S3::BucketPolicy")
    frontend_policy = next(
        policy
        for policy in bucket_policies.values()
        if "cloudfront.amazonaws.com" in json.dumps(policy)
    )
    policy_json = json.dumps(frontend_policy)
    assert "aws:SecureTransport" in policy_json
    assert "s3:GetObject" in policy_json


def test_cloudfront_serves_spa_over_oac_and_proxies_api_without_uri_logging() -> None:
    _, _, template = build_stack()
    distribution = next(iter(template.find_resources("AWS::CloudFront::Distribution").values()))
    config = distribution["Properties"]["DistributionConfig"]

    assert config["DefaultRootObject"] == "index.html"
    assert "Logging" not in config
    assert config["DefaultCacheBehavior"]["ViewerProtocolPolicy"] == "redirect-to-https"

    template.resource_count_is("AWS::CloudFront::OriginAccessControl", 1)

    api_behavior = next(
        behavior for behavior in config["CacheBehaviors"] if behavior["PathPattern"] == "/api/*"
    )
    assert api_behavior["ViewerProtocolPolicy"] == "redirect-to-https"
    # CachePolicyId matches the managed CACHING_DISABLED policy and the request policy is
    # the managed ALL_VIEWER_EXCEPT_HOST_HEADER policy so execute-api can resolve the stage.
    assert api_behavior["CachePolicyId"] == "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
    assert api_behavior["OriginRequestPolicyId"] == "b689b0a8-53d0-40ab-baf2-68738e2966ac"

    origins = config["Origins"]
    api_origin = next(origin for origin in origins if "CustomOriginConfig" in origin)
    assert api_origin["CustomOriginConfig"]["OriginProtocolPolicy"] == "https-only"
    assert "execute-api" in json.dumps(api_origin["DomainName"])


def test_cloudfront_response_headers_enforce_hsts_nosniff_and_frame_deny() -> None:
    _, _, template = build_stack()
    policy = next(iter(template.find_resources("AWS::CloudFront::ResponseHeadersPolicy").values()))
    security = policy["Properties"]["ResponseHeadersPolicyConfig"]["SecurityHeadersConfig"]

    assert security["ContentTypeOptions"]["Override"] is True
    assert security["FrameOptions"]["FrameOption"] == "DENY"
    hsts = security["StrictTransportSecurity"]
    assert hsts["AccessControlMaxAgeSec"] == 31536000
    assert hsts["IncludeSubdomains"] is True
    assert hsts["Preload"] is True


def test_frontend_deployment_publishes_dist_and_invalidates_distribution() -> None:
    _, _, template = build_stack()
    deployments = template.find_resources("Custom::CDKBucketDeployment")
    assert len(deployments) == 1
    deployment = next(iter(deployments.values()))["Properties"]
    assert "FrontendBucket" in json.dumps(deployment["DestinationBucketName"])
    assert deployment["DistributionPaths"] == ["/*"]
    assert "FrontendDistribution" in json.dumps(deployment["DistributionId"])
    assert deployment["Prune"] is True


def test_stack_outputs_explorer_api_auth_and_frontend_coordinates() -> None:
    _, _, template = build_stack()
    outputs = template.to_json()["Outputs"]
    for output in (
        "ExplorerFunctionName",
        "ExplorerApiEndpoint",
        "ExplorerApiStageName",
        "UserPoolId",
        "UserPoolClientId",
        "UserPoolHostedUiDomain",
        "FrontendBucketName",
        "FrontendDistributionId",
        "FrontendDistributionDomainName",
    ):
        assert output in outputs
    assert outputs["ExplorerApiStageName"]["Value"] == "api"


def _report_runner_function(template: Template) -> dict[str, Any]:
    return next(
        cast(dict[str, Any], function)
        for function in template.find_resources("AWS::Lambda::Function").values()
        if function["Properties"]["Handler"] == "src.report_runner.handler"
    )


def _role_by_description(template: Template, needle: str) -> str:
    roles = template.find_resources("AWS::IAM::Role")
    return next(
        logical_id
        for logical_id, role in roles.items()
        if needle in json.dumps(role.get("Properties", {}).get("Description", "")).lower()
    )


def _policy_for_role(template: Template, role_logical_id: str) -> dict[str, Any]:
    policies = template.find_resources("AWS::IAM::Policy")
    return next(
        cast(dict[str, Any], policy)
        for policy in policies.values()
        if any(
            role_logical_id in json.dumps(role_ref)
            for role_ref in policy["Properties"].get("Roles", [])
        )
    )


def _actions(policy: dict[str, Any]) -> set[str]:
    actions: set[str] = set()
    for statement in policy["Properties"]["PolicyDocument"]["Statement"]:
        action = statement["Action"]
        actions.update(action if isinstance(action, list) else [action])
    return actions


def test_reports_bucket_is_private_kms_versioned_logged_and_retained() -> None:
    _, _, template = build_stack()
    buckets = template.find_resources("AWS::S3::Bucket")
    reports = next(
        bucket
        for bucket in buckets.values()
        if bucket["Properties"].get("LoggingConfiguration", {}).get("LogFilePrefix") == "reports/"
    )
    properties = reports["Properties"]
    assert properties["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True,
    }
    assert properties["VersioningConfiguration"] == {"Status": "Enabled"}
    encryption = properties["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]
    assert encryption["ServerSideEncryptionByDefault"]["SSEAlgorithm"] == "aws:kms"
    assert reports["DeletionPolicy"] == "Retain"
    assert reports["UpdateReplacePolicy"] == "Retain"

    bucket_policies = template.find_resources("AWS::S3::BucketPolicy")
    reports_policy = next(
        policy
        for policy in bucket_policies.values()
        if "reports/" in json.dumps(policy) or "ReportsBucket" in json.dumps(policy)
    )
    policy_json = json.dumps(reports_policy)
    assert "aws:SecureTransport" in policy_json
    assert '"s3:TlsVersion": 1.2' in policy_json


def test_report_tables_are_pay_per_request_cmk_pitr_and_retained() -> None:
    _, _, template = build_stack()
    tables = template.find_resources("AWS::DynamoDB::Table")
    assert len(tables) == 3
    for table in tables.values():
        properties = table["Properties"]
        assert properties["BillingMode"] == "PAY_PER_REQUEST"
        assert properties["SSESpecification"]["SSEEnabled"] is True
        assert properties["SSESpecification"]["SSEType"] == "KMS"
        assert "KMSMasterKeyId" in properties["SSESpecification"]
        assert properties["PointInTimeRecoverySpecification"]["PointInTimeRecoveryEnabled"] is True
        assert table["DeletionPolicy"] == "Retain"
        assert table["UpdateReplacePolicy"] == "Retain"

    catalog = next(
        table
        for table in tables.values()
        if table["Properties"]["KeySchema"]
        == [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ]
    )
    assert "GlobalSecondaryIndexes" not in catalog["Properties"]
    # Row-granular edits pair each change with an append-only audit item in one
    # TransactWriteItems, so the catalog table carries no DynamoDB stream.
    assert "StreamSpecification" not in catalog["Properties"]

    runs = next(
        table
        for table in tables.values()
        if table["Properties"]["KeySchema"] == [{"AttributeName": "runId", "KeyType": "HASH"}]
    )
    index = runs["Properties"]["GlobalSecondaryIndexes"][0]
    assert index["KeySchema"] == [
        {"AttributeName": "reportId", "KeyType": "HASH"},
        {"AttributeName": "startedAt", "KeyType": "RANGE"},
    ]
    assert index["Projection"] == {"ProjectionType": "ALL"}


def test_report_runner_lambda_is_private_python314_arm64_bounded() -> None:
    _, _, template = build_stack()
    function = _report_runner_function(template)["Properties"]

    assert function["Runtime"] == "python3.14"
    assert function["Architectures"] == ["arm64"]
    assert function["Handler"] == "src.report_runner.handler"
    assert function["Timeout"] == 900
    assert function["MemorySize"] == 1024
    assert function["ReservedConcurrentExecutions"] == 2
    assert "VpcConfig" in function
    assert "KmsKeyArn" in function

    environment = function["Environment"]["Variables"]
    assert environment["OPENSEARCH_SERVICE"] == "aoss"
    assert "REPORT_BUCKET" in environment
    assert "REPORT_CATALOG_TABLE" in environment
    assert "RUNS_TABLE" in environment
    # Definitions now load from the catalog table, so the S3 definition wiring is gone.
    assert "DEFINITION_BUCKET" not in environment
    assert "DEFINITION_PREFIX" not in environment
    assert environment["OPENSEARCH_ENDPOINT"]
    assert not any("PASSWORD" in key for key in environment)


def test_report_runner_role_has_least_privilege_reports_access() -> None:
    _, _, template = build_stack()
    runner_role_id = _role_by_description(template, "report runner")
    policy = _policy_for_role(template, runner_role_id)
    actions = _actions(policy)

    assert "s3:PutObject" in actions
    assert "s3:GetObject" not in actions
    assert "s3:DeleteObject" not in actions
    # Definitions are read from the catalog table with a single partition Query.
    assert "dynamodb:Query" in actions
    assert "dynamodb:UpdateItem" in actions
    assert "dynamodb:GetItem" in actions
    # The runner records progress but never deletes runs or scans a table.
    assert "dynamodb:DeleteItem" not in actions
    assert "dynamodb:Scan" not in actions
    assert "aoss:APIAccessAll" in actions
    # The runner reads and writes objects but issues no presigned URLs and touches no Aurora.
    assert "rds-data:ExecuteStatement" not in actions
    assert "secretsmanager:GetSecretValue" not in actions

    policy_json = json.dumps(policy)
    assert "definitions/*" not in policy_json
    assert "outputs/*" in policy_json


def test_reports_read_only_index_access_for_runner_and_explorer() -> None:
    _, _, template = build_stack()
    access_policy = next(
        iter(template.find_resources("AWS::OpenSearchServerless::AccessPolicy").values())
    )
    access_json = json.dumps(access_policy)
    assert "ReportRunnerRole" in access_json
    assert "ExplorerRole" in access_json
    assert "aoss:ReadDocument" in access_json
    assert "aoss:DescribeIndex" in access_json
    # Read principals never receive write or delete document permissions.
    assert "aoss:DeleteDocument" not in access_json
    assert "aoss:*" not in access_json


def _resolve_policy_tokens(value: Any) -> str:
    """Reconstruct a ``Stack.to_json_string`` value into a parseable JSON string.

    Static segments render as literal strings while unresolved CloudFormation tokens (role
    ARNs delivered as ``Fn::GetAtt``/``Ref``) are replaced by their logical id so the whole
    document can be parsed and inspected rule by rule.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if "Fn::Join" in value:
            separator, parts = value["Fn::Join"]
            return cast(str, separator).join(_resolve_policy_tokens(part) for part in parts)
        if "Fn::GetAtt" in value:
            return cast(str, value["Fn::GetAtt"][0])
        if "Ref" in value:
            return cast(str, value["Ref"])
    pytest.fail("unexpected policy token")


def _statement_actions(statement: dict[str, Any]) -> list[str]:
    action = statement["Action"]
    return action if isinstance(action, list) else [action]


def test_message_search_reuses_explorer_read_only_index_grants() -> None:
    # The metadata attribute search rides the explorer's existing collection-scoped
    # APIAccessAll and the read-only data-access grant it already shares with the report
    # runner. This test pins those exact grants for both indexes so the search integration
    # can never silently widen or narrow the AOSS surface.
    _, _, template = build_stack()

    access_policy = next(
        iter(template.find_resources("AWS::OpenSearchServerless::AccessPolicy").values())
    )
    rules = json.loads(_resolve_policy_tokens(access_policy["Properties"]["Policy"]))
    read_rule = next(
        rule
        for rule in rules
        if rule["Description"] == "Report runner and explorer read-only index access"
    )
    inner = read_rule["Rules"][0]
    assert inner["ResourceType"] == "index"
    assert inner["Resource"] == [
        "index/manifest-medex-dev/hl7-messages-v1",
        "index/manifest-medex-dev/ccda-documents-v1",
    ]
    # Exactly DescribeIndex + ReadDocument -- no write, delete, create, or update reaches
    # the explorer principal that now also backs message search.
    assert inner["Permission"] == ["aoss:DescribeIndex", "aoss:ReadDocument"]
    principal_json = json.dumps(read_rule["Principal"])
    assert "ExplorerRole" in principal_json
    assert "ReportRunnerRole" in principal_json

    # The collection-scoped data-plane grant stays on the collection ARN, never a wildcard.
    explorer_role_id = next(
        logical_id
        for logical_id, role in template.find_resources("AWS::IAM::Role").items()
        if "explorer" in json.dumps(role.get("Properties", {}).get("Description", "")).lower()
    )
    explorer_policy = next(
        policy
        for policy in template.find_resources("AWS::IAM::Policy").values()
        if any(
            explorer_role_id in json.dumps(role_ref)
            for role_ref in policy["Properties"].get("Roles", [])
        )
    )
    statements = explorer_policy["Properties"]["PolicyDocument"]["Statement"]
    api_access = [
        statement
        for statement in statements
        if "aoss:APIAccessAll" in _statement_actions(statement)
    ]
    assert len(api_access) == 1
    resource = api_access[0]["Resource"]
    resource_json = json.dumps(resource)
    assert "SearchCollection" in resource_json
    assert "*" not in resource_json


def test_reports_routes_are_jwt_and_never_presign_downloads() -> None:
    _, _, template = build_stack()
    routes = template.find_resources("AWS::ApiGatewayV2::Route")
    report_routes = {
        route["Properties"]["RouteKey"]
        for route in routes.values()
        if "/reports" in route["Properties"]["RouteKey"]
        or "/runs" in route["Properties"]["RouteKey"]
        or "/facilities" in route["Properties"]["RouteKey"]
    }
    assert "GET /reports/{id}/runs" in report_routes
    assert "GET /reports/{id}/history" in report_routes
    assert "GET /runs/{runId}/download" in report_routes
    assert all(
        route["Properties"]["AuthorizationType"] == "JWT"
        for route in routes.values()
        if route["Properties"]["RouteKey"] in report_routes
    )

    # No role may mint presigned URLs; downloads stream through the authenticated Lambda.
    stack_policies = json.dumps(template.find_resources("AWS::IAM::Policy"))
    assert "s3:presign" not in stack_policies.lower()


def test_explorer_can_only_invoke_the_report_runner_and_reingest_planner() -> None:
    _, _, template = build_stack()
    explorer_role_id = _role_by_description(template, "explorer")
    policy = _policy_for_role(template, explorer_role_id)
    invoke_statements = [
        statement
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]
        if "lambda:InvokeFunction"
        in (statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]])
    ]
    # The explorer dispatches exactly two workers: the report runner and the reingest planner.
    assert len(invoke_statements) == 2
    invoke_json = json.dumps([statement["Resource"] for statement in invoke_statements])
    assert "ReportRunnerFunction" in invoke_json
    assert "ReingestPlannerFunction" in invoke_json
    # No other function may be invoked; the reindexer is driven only by its SQS event source.
    assert "ReindexerFunction" not in invoke_json


def test_stack_outputs_reports_coordinates() -> None:
    _, _, template = build_stack()
    outputs = template.to_json()["Outputs"]
    for output in (
        "ReportsBucketName",
        "ReportsCatalogTableName",
        "ReportRunsTableName",
        "ReportRunnerFunctionName",
    ):
        assert output in outputs


def test_no_definitions_prefix_remains_anywhere_in_the_stack() -> None:
    _, _, template = build_stack()
    template_json = json.dumps(template.to_json())
    assert "definitions/" not in template_json
    assert "DEFINITION_BUCKET" not in template_json
    assert "DEFINITION_PREFIX" not in template_json


def test_no_report_audit_stream_path_remains_and_only_sqs_mappings_exist() -> None:
    _, _, template = build_stack()

    # The row-granular audit trail is now written transactionally into the catalog table, so
    # no report-audit Lambda, DynamoDB stream, or stream event source may remain.
    handlers = {
        function["Properties"]["Handler"]
        for function in template.find_resources("AWS::Lambda::Function").values()
    }
    assert "src.report_audit_handler.handler" not in handlers

    template_json = json.dumps(template.to_json())
    assert "report-audit" not in template_json
    assert "ReportAudit" not in template_json
    assert "StreamSpecification" not in template_json
    assert "StreamViewType" not in template_json
    assert '"audit/' not in template_json

    # Ingestion and reindexing rely on exactly three SQS event source mappings and nothing else.
    mappings = template.find_resources("AWS::Lambda::EventSourceMapping")
    assert len(mappings) == 3
    for mapping in mappings.values():
        event_source = json.dumps(mapping["Properties"].get("EventSourceArn", {}))
        assert "Queue" in event_source
        assert "StreamArn" not in event_source


def _function_by_handler(template: Template, handler: str) -> dict[str, Any]:
    return next(
        cast(dict[str, Any], function)
        for function in template.find_resources("AWS::Lambda::Function").values()
        if function["Properties"]["Handler"] == handler
    )


def test_reindex_queue_has_dead_letter_queue_and_operational_alarms() -> None:
    _, _, template = build_stack()
    queues = template.find_resources("AWS::SQS::Queue")

    # The reindex work queue redrives to its dedicated dead-letter queue after five receives
    # and shares the KMS-encrypted, TLS-only posture of the ingestion queues.
    processing = [queue for queue in queues.values() if "RedrivePolicy" in queue["Properties"]]
    reindex_queue = next(
        queue
        for queue in processing
        if queue["Properties"]["RedrivePolicy"]["maxReceiveCount"] == 5
        and queue["Properties"]["VisibilityTimeout"] == 900
        and "KmsMasterKeyId" in queue["Properties"]
    )
    assert reindex_queue["Properties"]["MessageRetentionPeriod"] == 1_209_600

    alarms = template.find_resources("AWS::CloudWatch::Alarm")
    descriptions = {alarm["Properties"].get("AlarmDescription", "") for alarm in alarms.values()}
    assert any("REINDEX queue has unprocessed messages" in text for text in descriptions)
    assert any(
        "REINDEX dead-letter queue contains failed messages" in text for text in descriptions
    )


def test_reingest_jobs_table_is_pay_per_request_cmk_pitr_and_retained() -> None:
    _, _, template = build_stack()
    tables = template.find_resources("AWS::DynamoDB::Table")
    jobs = next(
        table
        for table in tables.values()
        if table["Properties"]["KeySchema"] == [{"AttributeName": "jobId", "KeyType": "HASH"}]
    )
    properties = jobs["Properties"]
    assert properties["BillingMode"] == "PAY_PER_REQUEST"
    assert properties["SSESpecification"]["SSEEnabled"] is True
    assert properties["SSESpecification"]["SSEType"] == "KMS"
    assert "KMSMasterKeyId" in properties["SSESpecification"]
    assert properties["PointInTimeRecoverySpecification"]["PointInTimeRecoveryEnabled"] is True
    assert jobs["DeletionPolicy"] == "Retain"
    assert jobs["UpdateReplacePolicy"] == "Retain"
    # The job row is keyed only by jobId; it carries no secondary index or stream.
    assert "GlobalSecondaryIndexes" not in properties
    assert "StreamSpecification" not in properties


def test_reingest_planner_lambda_is_private_python314_arm64_bounded() -> None:
    _, _, template = build_stack()
    function = _function_by_handler(template, "src.reingest_planner.handler")["Properties"]

    assert function["Runtime"] == "python3.14"
    assert function["Architectures"] == ["arm64"]
    assert function["Handler"] == "src.reingest_planner.handler"
    assert function["Timeout"] == 900
    assert function["ReservedConcurrentExecutions"] == 1
    assert "VpcConfig" in function
    assert "KmsKeyArn" in function

    environment = function["Environment"]["Variables"]
    assert "JOBS_TABLE" in environment
    assert environment["METADATA_DATABASE"] == "manifest_medex"
    assert environment["METADATA_TABLE"] == "document_metadata"
    assert "METADATA_CLUSTER_ARN" in environment
    assert "METADATA_SECRET_ARN" in environment
    assert "REINDEX_QUEUE_URL" in environment
    assert not any("PASSWORD" in key for key in environment)


def test_reindexer_lambda_is_private_python314_arm64_with_parser_versions() -> None:
    from src.ccda_parser import PARSER_VERSION as CCDA_VERSION
    from src.parser import PARSER_VERSION as HL7_VERSION

    _, _, template = build_stack()
    function = _function_by_handler(template, "src.reindexer_handler.handler")["Properties"]

    assert function["Runtime"] == "python3.14"
    assert function["Architectures"] == ["arm64"]
    assert function["Handler"] == "src.reindexer_handler.handler"
    assert function["Timeout"] == 300
    assert function["ReservedConcurrentExecutions"] == 5
    assert "VpcConfig" in function
    assert "KmsKeyArn" in function

    environment = function["Environment"]["Variables"]
    assert "PARSED_BUCKET" in environment
    assert "JOBS_TABLE" in environment
    assert environment["OPENSEARCH_ENDPOINT"]
    assert environment["OPENSEARCH_SERVICE"] == "aoss"
    assert environment["OPENSEARCH_HL7_INDEX"] == "hl7-messages-v1"
    assert environment["OPENSEARCH_CCDA_INDEX"] == "ccda-documents-v1"
    assert environment["MAX_RECEIVE_COUNT"] == "5"
    # Parser versions are injected from the parser modules at synth so the reindexer never
    # imports a parser at runtime yet can still flag stale parses.
    assert environment["HL7_PARSER_VERSION"] == HL7_VERSION
    assert environment["CCDA_PARSER_VERSION"] == CCDA_VERSION
    assert not any("PASSWORD" in key for key in environment)


def test_reindexer_event_source_is_bounded_and_reports_partial_batch_failures() -> None:
    _, _, template = build_stack()
    reindexer = _function_by_handler(template, "src.reindexer_handler.handler")
    reindexer_ref = json.dumps(reindexer)
    reindexer_logical_id = next(
        logical_id
        for logical_id, function in template.find_resources("AWS::Lambda::Function").items()
        if function["Properties"]["Handler"] == "src.reindexer_handler.handler"
    )
    mapping = next(
        mapping
        for mapping in template.find_resources("AWS::Lambda::EventSourceMapping").values()
        if reindexer_logical_id in json.dumps(mapping["Properties"].get("FunctionName", {}))
    )
    properties = mapping["Properties"]
    assert properties["BatchSize"] == 10
    assert properties["ScalingConfig"]["MaximumConcurrency"] == 5
    assert properties["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert "ReindexQueue" in json.dumps(properties["EventSourceArn"])
    assert reindexer_ref  # sanity: the reindexer function resolved


def test_reingest_planner_role_has_least_privilege_dispatch_access() -> None:
    _, _, template = build_stack()
    planner_role_id = _role_by_description(template, "reingestion planner")
    policy = _policy_for_role(template, planner_role_id)
    actions = _actions(policy)
    policy_json = json.dumps(policy)

    # The planner reads metadata over the Data API, reads the Aurora secret, drives the job
    # row, and enqueues reindex work. It touches nothing else.
    assert "rds-data:ExecuteStatement" in actions
    assert "secretsmanager:GetSecretValue" in actions
    assert "dynamodb:UpdateItem" in actions
    assert "dynamodb:GetItem" in actions
    assert "sqs:SendMessage" in actions
    assert "kms:Decrypt" in actions

    # No batch statements, no document reads/writes, no OpenSearch, no queue consumption.
    assert "rds-data:BatchExecuteStatement" not in actions
    assert "dynamodb:DeleteItem" not in actions
    assert "s3:GetObject" not in actions
    assert "s3:PutObject" not in actions
    assert "aoss:APIAccessAll" not in actions
    assert "sqs:ReceiveMessage" not in actions

    # It reaches only the reindex queue, the jobs table, and the metadata cluster/secret,
    # never the raw or parsed buckets or the parser ingestion queues.
    assert "ReindexQueue" in policy_json
    assert "ReingestJobsTable" in policy_json
    assert "RawBucket" not in policy_json
    assert "ParsedBucket" not in policy_json
    assert "Hl7Queue" not in policy_json
    assert "CcdaQueue" not in policy_json


def test_reindexer_role_has_least_privilege_write_access() -> None:
    _, _, template = build_stack()
    reindexer_role_id = _role_by_description(template, "parsed-zone reindexer")
    policy = _policy_for_role(template, reindexer_role_id)
    actions = _actions(policy)
    policy_json = json.dumps(policy)

    # Read-only, version-aware parsed-object access; atomic counter writes; collection data
    # plane; and Decrypt for the parsed object and the encrypted queue.
    assert "s3:GetObject" in actions
    assert "s3:GetObjectVersion" in actions
    assert "dynamodb:UpdateItem" in actions
    assert "aoss:APIAccessAll" in actions
    assert "kms:Decrypt" in actions
    # The SQS event source generates the queue consume grants on this role.
    assert "sqs:ReceiveMessage" in actions
    assert "sqs:DeleteMessage" in actions

    # The reindexer never writes or deletes objects, never reads the job row, never sends to
    # a queue, never uses the Data API or the Aurora secret, and never encrypts with the CMK.
    assert "s3:PutObject" not in actions
    assert "s3:DeleteObject" not in actions
    assert "dynamodb:GetItem" not in actions
    assert "dynamodb:DeleteItem" not in actions
    assert "sqs:SendMessage" not in actions
    assert "rds-data:ExecuteStatement" not in actions
    assert "secretsmanager:GetSecretValue" not in actions
    assert "kms:Encrypt" not in actions
    assert "kms:GenerateDataKey" not in policy_json

    # It reaches only the parsed bucket, the jobs table, and the reindex queue -- never raw
    # source, the metadata cluster, or the parser ingestion queues.
    assert "RawBucket" not in policy_json
    assert "MetadataCluster" not in policy_json
    assert "Hl7Queue" not in policy_json
    assert "CcdaQueue" not in policy_json


def test_reindexer_has_a_separate_write_only_aoss_data_rule() -> None:
    _, _, template = build_stack()
    access_policy = next(
        iter(template.find_resources("AWS::OpenSearchServerless::AccessPolicy").values())
    )
    rules = json.loads(_resolve_policy_tokens(access_policy["Properties"]["Policy"]))
    write_rule = next(
        rule
        for rule in rules
        if rule["Description"] == "Reindexer parsed-zone document write access"
    )
    inner = write_rule["Rules"][0]
    assert inner["ResourceType"] == "index"
    assert inner["Resource"] == [
        "index/manifest-medex-dev/hl7-messages-v1",
        "index/manifest-medex-dev/ccda-documents-v1",
    ]
    # DescribeIndex + WriteDocument for the bulk write path, plus UpdateIndex because indexing
    # a new field (originalIngestTime) updates the dynamic mapping; no ReadDocument, CreateIndex,
    # or DeleteDocument reach the reindexer principal.
    assert inner["Permission"] == [
        "aoss:DescribeIndex",
        "aoss:WriteDocument",
        "aoss:UpdateIndex",
    ]
    assert "aoss:ReadDocument" not in inner["Permission"]
    assert "aoss:CreateIndex" not in inner["Permission"]
    assert "aoss:DeleteDocument" not in inner["Permission"]

    principal_json = json.dumps(write_rule["Principal"])
    assert "ReindexerRole" in principal_json
    # The reindexer must not appear in either read-only or ingestion write rules.
    read_rule = next(
        rule
        for rule in rules
        if rule["Description"] == "Report runner and explorer read-only index access"
    )
    assert "ReindexerRole" not in json.dumps(read_rule["Principal"])


def test_reingest_routes_are_jwt_authenticated() -> None:
    _, _, template = build_stack()
    routes = template.find_resources("AWS::ApiGatewayV2::Route")
    reingest_routes = {
        route["Properties"]["RouteKey"]
        for route in routes.values()
        if "/reingest" in route["Properties"]["RouteKey"]
    }
    assert reingest_routes == {
        "POST /reingest/preview",
        "POST /reingest/jobs",
        "GET /reingest/jobs",
        "GET /reingest/jobs/{jobId}",
    }
    assert all(
        route["Properties"]["AuthorizationType"] == "JWT"
        for route in routes.values()
        if route["Properties"]["RouteKey"] in reingest_routes
    )


def test_explorer_environment_carries_reingest_coordinates() -> None:
    _, _, template = build_stack()
    function = _explorer_function(template)["Properties"]
    environment = function["Environment"]["Variables"]
    assert "REINGEST_JOBS_TABLE" in environment
    assert "REINGEST_PLANNER_FUNCTION" in environment


def test_stack_outputs_reingest_coordinates() -> None:
    _, _, template = build_stack()
    outputs = template.to_json()["Outputs"]
    for output in (
        "ReindexQueueUrl",
        "ReingestJobsTableName",
        "ReingestPlannerFunctionName",
        "ReindexerFunctionName",
    ):
        assert output in outputs
