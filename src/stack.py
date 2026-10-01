"""Define the private, format-specific AWS ingestion platform with CDK."""

from importlib import metadata
from pathlib import Path
from shutil import copy2, copytree, ignore_patterns
from typing import Any, ClassVar, cast

import jsii
from aws_cdk import (
    Aws,
    BundlingOptions,
    CfnDeletionPolicy,
    CfnOutput,
    CfnResource,
    Duration,
    Environment,
    ILocalBundling,
    RemovalPolicy,
    Size,
    Stack,
    Tags,
)
from aws_cdk import aws_apigatewayv2 as apigwv2
from aws_cdk import aws_apigatewayv2_authorizers as apigwv2_authorizers
from aws_cdk import aws_apigatewayv2_integrations as apigwv2_integrations
from aws_cdk import aws_cloudfront as cloudfront
from aws_cdk import aws_cloudfront_origins as cloudfront_origins
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cognito as cognito
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_opensearchserverless as opensearchserverless
from aws_cdk import aws_rds as rds
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deployment
from aws_cdk import aws_sqs as sqs
from cdk_nag import NagSuppressions
from constructs import Construct
from src.ccda_parser import PARSER_VERSION as CCDA_PARSER_VERSION
from src.config import AppConfig, DeploymentEnvironment
from src.parser import PARSER_VERSION as HL7_PARSER_VERSION

METADATA_DATABASE_NAME = "manifest_medex"
METADATA_TABLE_NAME = "document_metadata"
REPORT_RUNS_INDEX_NAME = "reportId-startedAt-index"
REPORT_OUTPUT_PREFIX = "outputs/"
HL7_INDEX_NAME = "hl7-messages-v1"
CCDA_INDEX_NAME = "ccda-documents-v1"
AURORA_CREDENTIALS_MISSING = "Aurora generated credentials did not produce a secret"
MAX_EXPLORER_BODY_BYTES = 4 * 1024 * 1024
FRONTEND_DIST_MISSING = (
    "Frontend build output was not found at web/dist; run 'make web-build' before synthesis"
)


@jsii.implements(ILocalBundling)
class _LambdaBundler:
    """Bundle source with exact pure-Python runtime dependencies from uv."""

    _DEPENDENCIES: ClassVar[dict[str, tuple[str, str]]] = {
        "defusedxml": ("0.7.1", "defusedxml"),
        "hl7": ("0.4.5", "hl7"),
    }
    _PROJECT_ROOT = Path(__file__).resolve().parents[1]

    def try_bundle(self, output_dir: str, _options: BundlingOptions) -> bool:
        # Local bundling is valid only when installed packages exactly match runtime pins.
        distributions: dict[str, metadata.Distribution] = {}
        try:
            for name, (version, _) in self._DEPENDENCIES.items():
                distribution = metadata.distribution(name)
                if distribution.version != version:
                    return False
                distributions[name] = distribution
        except metadata.PackageNotFoundError:
            return False

        destination = Path(output_dir)
        copytree(
            self._PROJECT_ROOT / "src",
            destination / "src",
            dirs_exist_ok=True,
            ignore=ignore_patterns("__pycache__", "*.pyc", "stack.py", "config.py"),
        )
        # report_definition loads its JSON Schema from the project-root schema/ directory
        # (outside src/), so the runtime asset must carry that directory verbatim.
        copytree(
            self._PROJECT_ROOT / "schema",
            destination / "schema",
            dirs_exist_ok=True,
            ignore=ignore_patterns("__pycache__", "*.pyc"),
        )
        for name, (_, package_name) in self._DEPENDENCIES.items():
            distribution = distributions[name]
            package = Path(str(distribution.locate_file(package_name)))
            copytree(
                package,
                destination / package_name,
                dirs_exist_ok=True,
                ignore=ignore_patterns("__pycache__", "*.pyc"),
            )
            for entry in distribution.files or ():
                if entry.parts and entry.parts[0].endswith(".dist-info"):
                    source = Path(str(distribution.locate_file(entry)))
                    target = destination / entry
                    target.parent.mkdir(parents=True, exist_ok=True)
                    if source.is_file():
                        copy2(source, target)
        return True


class DataQualityStack(Stack):
    """Private format-specific clinical ingestion and metadata platform."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: AppConfig,
        env: Environment | None = None,
    ) -> None:
        self.config = config
        self.stack_prefix = f"{config.project_name}-{config.environment.value}"
        super().__init__(
            scope,
            construct_id,
            description="Manifest MedEx private HL7 and CCDA ingestion platform",
            env=env,
            stack_name=self.stack_prefix,
            termination_protection=config.termination_protection,
        )

        # Build foundational storage/network resources before consumers that reference them.
        self._add_standard_tags()
        self.encryption_key = self._create_encryption_key()
        self.access_logs_bucket = self._create_access_logs_bucket()
        self.raw_bucket = self._create_data_bucket("RawBucket", "raw/")
        self.parsed_bucket = self._create_data_bucket("ParsedBucket", "parsed/")
        self.error_bucket = self._create_data_bucket("ErrorBucket", "error/")
        self._enable_raw_bucket_event_bridge()

        self.hl7_queue, self.hl7_dead_letter_queue = self._create_queue_pair("Hl7")
        self.ccda_queue, self.ccda_dead_letter_queue = self._create_queue_pair("Ccda")

        self.vpc = self._create_ingestion_vpc()
        self.ingestion_security_group = ec2.SecurityGroup(
            self,
            "IngestionSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Network access for the format-specific ingestion Lambdas",
        )
        self.serverless_endpoint_security_group = self._create_endpoint_security_group(
            "ServerlessEndpointSecurityGroup",
            "OpenSearch Serverless",
        )
        self.data_api_endpoint_security_group = self._create_endpoint_security_group(
            "DataApiEndpointSecurityGroup",
            "RDS Data API",
        )
        self.sqs_endpoint_security_group = self._create_endpoint_security_group(
            "SqsEndpointSecurityGroup",
            "Amazon SQS",
            # Ingestion Lambdas are queue-invoked and never send; only the planner needs SQS.
            allow_ingestion=False,
        )

        self.hl7_role = self._create_ingestion_role("Hl7", "incoming/hl7/*", "hl7/*")
        self.ccda_role = self._create_ingestion_role("Ccda", "incoming/ccda/*", "ccda/*")

        # Reports storage and metadata are provisioned before their consumer roles.
        self.reports_bucket = self._create_data_bucket("ReportsBucket", "reports/")
        self.reports_catalog_table = self._create_reports_catalog_table()
        self.report_runs_table = self._create_report_runs_table()

        # Reingestion queue, jobs table, and networking are provisioned before the roles and
        # search collection that reference them, so the reindexer can join the data policy.
        self.reindex_queue, self.reindex_dead_letter_queue = self._create_queue_pair("Reindex")
        self.reingest_jobs_table = self._create_reingest_jobs_table()

        # The report runner and explorer roles exist before the search collection so the
        # data access policy can grant both read-only index access in one document.
        self.report_runner_security_group = self._create_report_runner_security_group()
        self.explorer_security_group = self._create_explorer_security_group()
        self.reingest_planner_security_group = self._create_reingest_planner_security_group()
        self.reindexer_security_group = self._create_reindexer_security_group()
        self.report_runner_role = self._create_report_runner_role()
        self.explorer_role = self._create_explorer_role()
        self.reingest_planner_role = self._create_reingest_planner_role()
        self.reindexer_role = self._create_reindexer_role()

        self.serverless_vpc_endpoint = self._create_serverless_vpc_endpoint()
        self.search_collection = self._create_search_collection()
        self._grant_collection_access()

        self.metadata_cluster = self._create_metadata_cluster()
        NagSuppressions.add_resource_suppressions_by_path(
            self,
            f"/{self.node.path}/MetadataCluster/Secret/Resource",
            [
                {
                    "id": "AwsSolutions-SMG4",
                    "reason": (
                        "The v1 Data API-only design intentionally has no database-connected "
                        "rotation Lambda or port 5432 ingress; rotate this retained demo secret "
                        "through an explicitly approved maintenance operation."
                    ),
                }
            ],
        )
        self.data_api_endpoint = self._create_data_api_endpoint()
        self.sqs_endpoint = self._create_sqs_endpoint()
        self._grant_metadata_access(self.hl7_role)
        self._grant_metadata_access(self.ccda_role)

        self.hl7_function = self._create_ingestion_function(
            "Hl7",
            handler="src.hl7_handler.handler",
            role=self.hl7_role,
            source_format="hl7-v2",
            reserved_concurrency=10,
        )
        self.ccda_function = self._create_ingestion_function(
            "Ccda",
            handler="src.ccda_handler.handler",
            role=self.ccda_role,
            source_format="ccda",
            reserved_concurrency=5,
        )

        self.hl7_raw_object_rule = self._create_raw_object_rule(
            "Hl7", "incoming/hl7/", self.hl7_queue, self.hl7_dead_letter_queue
        )
        self.ccda_raw_object_rule = self._create_raw_object_rule(
            "Ccda", "incoming/ccda/", self.ccda_queue, self.ccda_dead_letter_queue
        )
        self._connect_event_sources()
        self._create_operational_alarms()

        # Authenticated read-only explorer API and CloudFront/S3 single-page frontend.
        self.report_runner_function = self._create_report_runner_function()
        self._authorize_explorer_data_api()
        self._authorize_report_network()
        # Reingestion planner and parsed-zone reindexer are wired before the explorer, which
        # dispatches the planner and reads the jobs table through injected environment.
        self._authorize_reingest_planner_data_api()
        self._authorize_reindexer_network()
        self.reingest_planner_function = self._create_reingest_planner_function()
        self.reindexer_function = self._create_reindexer_function()
        self._connect_reindex_event_source()
        self._authorize_explorer_reingest()
        self.explorer_function = self._create_explorer_function()
        self.http_api = self._create_http_api()
        self.frontend_bucket = self._create_frontend_bucket()
        self.distribution = self._create_distribution()
        self.user_pool, self.user_pool_client, self.user_pool_domain = self._create_user_pool()
        self._configure_explorer_routes()
        self.http_stage = self._create_http_stage()
        self._deploy_frontend()

        self._create_outputs()

    def _add_standard_tags(self) -> None:
        tags = {
            "Application": self.config.project_name,
            "DataClassification": "PHI",
            "Environment": self.config.environment.value,
            "ManagedBy": "AWS-CDK",
        }
        for key, value in tags.items():
            Tags.of(self).add(key, value)

    def _create_encryption_key(self) -> kms.Key:
        key = kms.Key(
            self,
            "EncryptionKey",
            alias=f"alias/{self.stack_prefix}",
            description="Encrypts Manifest MedEx data, messages, logs, secrets, and Aurora",
            enable_key_rotation=True,
            pending_window=Duration.days(30),
            removal_policy=RemovalPolicy.RETAIN,
        )
        log_group_arns = [
            f"arn:{self.partition}:logs:{self.region}:{self.account}:log-group:{name}"
            for name in (
                f"/aws/vpc/{self.stack_prefix}",
                f"/aws/lambda/{self.stack_prefix}-hl7",
                f"/aws/lambda/{self.stack_prefix}-ccda",
                f"/aws/lambda/{self.stack_prefix}-explorer",
                f"/aws/lambda/{self.stack_prefix}-report-runner",
                f"/aws/lambda/{self.stack_prefix}-reingest-planner",
                f"/aws/lambda/{self.stack_prefix}-reindexer",
            )
        ]
        key.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowCloudWatchLogsForStackLogGroups",
                principals=[iam.ServicePrincipal(f"logs.{self.region}.{self.url_suffix}")],
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:Describe*",
                ],
                resources=["*"],
                conditions={"ArnEquals": {"kms:EncryptionContext:aws:logs:arn": log_group_arns}},
            )
        )
        NagSuppressions.add_resource_suppressions(
            key,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "KMS key policies require Resource '*' to refer to the key itself; "
                        "service and role principals remain explicitly scoped."
                    ),
                }
            ],
        )
        return key

    def _create_access_logs_bucket(self) -> s3.Bucket:
        bucket = s3.Bucket(
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
        NagSuppressions.add_resource_suppressions(
            bucket,
            [
                {
                    "id": "AwsSolutions-S1",
                    "reason": (
                        "The dedicated access-log destination cannot recursively log to itself."
                    ),
                }
            ],
        )
        return bucket

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

    def _create_reports_catalog_table(self) -> dynamodb.Table:
        # Row-granular Reports store every report as items under one partition key (PK) with a
        # per-item sort key (SK) for the META header, section headers, and definition rows.
        # Each mutation pairs the change with an append-only audit item in one
        # TransactWriteItems, so no table stream or audit consumer is required.
        return dynamodb.Table(
            self,
            "ReportsCatalogTable",
            table_name=f"{self.stack_prefix}-reports-catalog",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.encryption_key,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )

    def _create_report_runs_table(self) -> dynamodb.Table:
        table = dynamodb.Table(
            self,
            "ReportRunsTable",
            table_name=f"{self.stack_prefix}-report-runs",
            partition_key=dynamodb.Attribute(name="runId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.encryption_key,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )
        # Runs are listed newest-first per report through a report-id/started-at index.
        table.add_global_secondary_index(
            index_name=REPORT_RUNS_INDEX_NAME,
            partition_key=dynamodb.Attribute(name="reportId", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="startedAt", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        return table

    def _create_reingest_jobs_table(self) -> dynamodb.Table:
        # One row per reingestion job keyed solely by jobId. The planner and the parsed-zone
        # reindexer advance atomic counters and the lifecycle status in place; the row never
        # stores the ID list or a stream, so no secondary index or consumer is required.
        return dynamodb.Table(
            self,
            "ReingestJobsTable",
            table_name=f"{self.stack_prefix}-reingest-jobs",
            partition_key=dynamodb.Attribute(name="jobId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.encryption_key,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )

    def _enable_raw_bucket_event_bridge(self) -> None:
        raw_bucket_resource = cast(s3.CfnBucket, self.raw_bucket.node.default_child)
        raw_bucket_resource.notification_configuration = (
            s3.CfnBucket.NotificationConfigurationProperty(
                event_bridge_configuration=s3.CfnBucket.EventBridgeConfigurationProperty(
                    event_bridge_enabled=True
                )
            )
        )

    def _create_queue_pair(self, construct_id: str) -> tuple[sqs.Queue, sqs.Queue]:
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
            visibility_timeout=Duration.minutes(15),
        )
        return queue, dead_letter_queue

    def _create_raw_object_rule(
        self,
        construct_id: str,
        prefix: str,
        queue: sqs.Queue,
        dead_letter_queue: sqs.Queue,
    ) -> events.Rule:
        rule = events.Rule(
            self,
            f"{construct_id}RawObjectRule",
            description=(
                f"Routes newly created {construct_id.upper()} objects to its ingestion queue"
            ),
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [self.raw_bucket.bucket_name]},
                    "object": {"key": [{"prefix": prefix}]},
                },
            ),
        )
        rule.add_target(
            events_targets.SqsQueue(
                queue,
                dead_letter_queue=dead_letter_queue,
                max_event_age=Duration.hours(2),
                retry_attempts=10,
            )
        )
        return rule

    def _create_ingestion_vpc(self) -> ec2.Vpc:
        availability_zones = 3 if self.config.environment is DeploymentEnvironment.PROD else 2
        # Isolated subnets and zero NAT gateways force service traffic through endpoints.
        vpc = ec2.Vpc(
            self,
            "IngestionVpc",
            ip_addresses=ec2.IpAddresses.cidr("10.42.0.0/16"),
            max_azs=availability_zones,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Ingestion",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                )
            ],
        )
        vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)
        vpc.add_gateway_endpoint(
            "DynamoDbEndpoint", service=ec2.GatewayVpcEndpointAwsService.DYNAMODB
        )
        flow_log_group = self._create_log_group(
            "VpcFlowLogGroup",
            f"/aws/vpc/{self.stack_prefix}",
        )
        vpc.add_flow_log(
            "FlowLog",
            destination=ec2.FlowLogDestination.to_cloud_watch_logs(flow_log_group),
            traffic_type=ec2.FlowLogTrafficType.REJECT,
        )
        return vpc

    def _create_endpoint_security_group(
        self,
        construct_id: str,
        service_name: str,
        *,
        allow_ingestion: bool = True,
    ) -> ec2.SecurityGroup:
        security_group = ec2.SecurityGroup(
            self,
            construct_id,
            vpc=self.vpc,
            allow_all_outbound=False,
            description=f"Allows HTTPS to {service_name} only from authorized Lambdas",
        )
        if allow_ingestion:
            security_group.add_ingress_rule(
                self.ingestion_security_group,
                ec2.Port.tcp(443),
                f"HTTPS from ingestion Lambdas to {service_name}",
            )
        return security_group

    def _create_ingestion_role(
        self,
        construct_id: str,
        raw_key_pattern: str,
        parsed_key_pattern: str,
    ) -> iam.Role:
        role = iam.Role(
            self,
            f"{construct_id}IngestionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description=f"Least-privilege execution role for {construct_id.upper()} ingestion",
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:UnassignPrivateIpAddresses",
                ],
                resources=["*"],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[self.raw_bucket.arn_for_objects(raw_key_pattern)],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[
                    self.parsed_bucket.arn_for_objects(parsed_key_pattern),
                    self.error_bucket.arn_for_objects(f"processing/{construct_id.lower()}/*"),
                ],
            )
        )
        self.encryption_key.grant_encrypt_decrypt(role)
        NagSuppressions.add_resource_suppressions(
            role,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "Lambda VPC APIs require wildcard resources; S3 object and log-stream "
                        "permissions use suffix wildcards within named prefixes; KMS APIs use "
                        "service-defined wildcards."
                    ),
                }
            ],
            apply_to_children=True,
        )
        return role

    def _create_serverless_vpc_endpoint(self) -> opensearchserverless.CfnVpcEndpoint:
        return opensearchserverless.CfnVpcEndpoint(
            self,
            "ServerlessVpcEndpoint",
            name=f"{self._search_collection_name()}-vpce",
            vpc_id=self.vpc.vpc_id,
            subnet_ids=[subnet.subnet_id for subnet in self.vpc.isolated_subnets],
            security_group_ids=[self.serverless_endpoint_security_group.security_group_id],
        )

    def _create_search_collection(self) -> opensearchserverless.CfnCollection:
        collection_name = self._search_collection_name()
        encryption_policy = opensearchserverless.CfnSecurityPolicy(
            self,
            "SearchEncryptionPolicy",
            name=f"{collection_name}-encryption",
            type="encryption",
            description="Customer-managed KMS encryption for the clinical search collection",
            policy=self.to_json_string(
                {
                    "Rules": [
                        {
                            "ResourceType": "collection",
                            "Resource": [f"collection/{collection_name}"],
                        }
                    ],
                    "AWSOwnedKey": False,
                    "KmsARN": self.encryption_key.key_arn,
                }
            ),
        )
        # The collection API always stays private; only the development Dashboard may opt out.
        private_network_resources: list[dict[str, Any]] = [
            {
                "ResourceType": "collection",
                "Resource": [f"collection/{collection_name}"],
            }
        ]
        if not self.config.enable_public_dashboard:
            private_network_resources.append(
                {
                    "ResourceType": "dashboard",
                    "Resource": [f"collection/{collection_name}"],
                }
            )
        network_rules: list[dict[str, Any]] = [
            {
                "Description": "Private API access from the dedicated VPC endpoint",
                "Rules": private_network_resources,
                "AllowFromPublic": False,
                "SourceVPCEs": [self.serverless_vpc_endpoint.attr_id],
            }
        ]
        if self.config.enable_public_dashboard:
            network_rules.append(
                {
                    "Description": "Temporary public development Dashboard browser access",
                    "Rules": [
                        {
                            "ResourceType": "dashboard",
                            "Resource": [f"collection/{collection_name}"],
                        }
                    ],
                    "AllowFromPublic": True,
                }
            )
        network_policy = opensearchserverless.CfnSecurityPolicy(
            self,
            "SearchNetworkPolicy",
            name=f"{collection_name}-network",
            type="network",
            description="Private collection API with context-controlled Dashboard access",
            policy=self.to_json_string(network_rules),
        )
        network_policy.add_resource_dependency(self.serverless_vpc_endpoint)

        index_resources = [
            f"index/{collection_name}/hl7-messages-v1",
            f"index/{collection_name}/ccda-documents-v1",
        ]
        access_rules: list[dict[str, Any]] = [
            {
                "Description": "Deterministic index creation and document writes",
                "Rules": [
                    {
                        "ResourceType": "index",
                        "Resource": index_resources,
                        "Permission": [
                            "aoss:CreateIndex",
                            "aoss:DescribeIndex",
                            "aoss:UpdateIndex",
                            "aoss:WriteDocument",
                        ],
                    }
                ],
                "Principal": [self.hl7_role.role_arn, self.ccda_role.role_arn],
            }
        ]
        # Report generation and the explorer facility lookup read both indexes only.
        access_rules.append(
            {
                "Description": "Report runner and explorer read-only index access",
                "Rules": [
                    {
                        "ResourceType": "index",
                        "Resource": index_resources,
                        "Permission": ["aoss:DescribeIndex", "aoss:ReadDocument"],
                    }
                ],
                "Principal": [
                    self.report_runner_role.role_arn,
                    self.explorer_role.role_arn,
                ],
            }
        )
        # The parsed-zone reindexer writes already-parsed documents back into both indexes.
        # It resolves an existing index (DescribeIndex) and bulk-writes documents
        # (WriteDocument). UpdateIndex is required because the added originalIngestTime field
        # updates the dynamic mapping on first write. It never needs ReadDocument or CreateIndex.
        access_rules.append(
            {
                "Description": "Reindexer parsed-zone document write access",
                "Rules": [
                    {
                        "ResourceType": "index",
                        "Resource": index_resources,
                        "Permission": [
                            "aoss:DescribeIndex",
                            "aoss:WriteDocument",
                            "aoss:UpdateIndex",
                        ],
                    }
                ],
                "Principal": [self.reindexer_role.role_arn],
            }
        )
        if self.config.enable_public_dashboard:
            access_rules.append(
                {
                    "Description": "Temporary read-only development Dashboard access",
                    "Rules": [
                        {
                            "ResourceType": "index",
                            "Resource": index_resources,
                            "Permission": ["aoss:DescribeIndex", "aoss:ReadDocument"],
                        }
                    ],
                    "Principal": [cast(str, self.config.dashboard_principal_arn)],
                }
            )
        access_policy = opensearchserverless.CfnAccessPolicy(
            self,
            "SearchDataAccessPolicy",
            name=f"{collection_name}-access",
            type="data",
            description="Format-specific ingestion write and optional Dashboard read access",
            policy=self.to_json_string(access_rules),
        )

        collection = opensearchserverless.CfnCollection(
            self,
            "SearchCollection",
            name=collection_name,
            type="SEARCH",
            description="Private search collection for deterministic HL7 and CCDA documents",
            deletion_protection="ENABLED",
            standby_replicas=(
                "ENABLED" if self.config.environment is DeploymentEnvironment.PROD else "DISABLED"
            ),
        )
        # Retain the collection through stack deletion and CloudFormation replacement.
        collection.cfn_options.deletion_policy = CfnDeletionPolicy.RETAIN
        collection.cfn_options.update_replace_policy = CfnDeletionPolicy.RETAIN
        collection.add_resource_dependency(encryption_policy)
        collection.add_resource_dependency(network_policy)
        access_policy.node.add_dependency(collection)
        return collection

    def _grant_collection_access(self) -> None:
        for role in (
            self.hl7_role,
            self.ccda_role,
            self.report_runner_role,
            self.explorer_role,
            self.reindexer_role,
        ):
            role.add_to_policy(
                iam.PolicyStatement(
                    actions=["aoss:APIAccessAll"],
                    resources=[self.search_collection.attr_arn],
                )
            )

    def _create_metadata_cluster(self) -> rds.DatabaseCluster:
        cluster = rds.DatabaseCluster(
            self,
            "MetadataCluster",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_16_8
            ),
            writer=rds.ClusterInstance.serverless_v2(
                "Writer",
                publicly_accessible=False,
            ),
            readers=[],
            credentials=rds.Credentials.from_generated_secret(
                "metadata_admin",
                encryption_key=self.encryption_key,
                secret_name=f"{self.stack_prefix}/aurora/metadata-admin",
            ),
            default_database_name=METADATA_DATABASE_NAME,
            enable_data_api=True,
            iam_authentication=True,
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=2,
            storage_encrypted=True,
            storage_encryption_key=self.encryption_key,
            backup=rds.BackupProps(
                retention=Duration.days(
                    35 if self.config.environment is DeploymentEnvironment.PROD else 7
                )
            ),
            copy_tags_to_snapshot=True,
            deletion_protection=self.config.environment is DeploymentEnvironment.PROD,
            removal_policy=RemovalPolicy.RETAIN,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )
        if cluster.secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        cluster.secret.apply_removal_policy(RemovalPolicy.RETAIN)
        credential_secret = cluster.secret.node.scope
        if credential_secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        secret_resource = cast(CfnResource, credential_secret.node.default_child)
        secret_resource.apply_removal_policy(RemovalPolicy.RETAIN)
        NagSuppressions.add_resource_suppressions(
            cluster,
            [
                {
                    "id": "AwsSolutions-RDS10",
                    "reason": (
                        "Development and staging retain the cluster on stack deletion but permit "
                        "explicit replacement; production enables deletion protection."
                    ),
                }
            ],
        )
        return cluster

    def _create_data_api_endpoint(self) -> ec2.InterfaceVpcEndpoint:
        # Lambdas use HTTPS Data API calls; no PostgreSQL socket path or port 5432 rule exists.
        endpoint = self.vpc.add_interface_endpoint(
            "RdsDataEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.RDS_DATA,
            private_dns_enabled=True,
            open=False,
            security_groups=[self.data_api_endpoint_security_group],
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )
        endpoint.add_to_policy(
            iam.PolicyStatement(
                principals=[self.hl7_role, self.ccda_role],
                actions=["rds-data:ExecuteStatement", "rds-data:BatchExecuteStatement"],
                resources=[self.metadata_cluster.cluster_arn],
            )
        )
        return endpoint

    def _create_sqs_endpoint(self) -> ec2.InterfaceVpcEndpoint:
        # The reingestion planner is the only Lambda that *sends* to SQS from inside the
        # isolated subnets (every other function is invoked by a queue, which needs no
        # outbound path). Without this endpoint, SendMessage hangs through boto retries
        # and the job never advances past its expected count. The endpoint policy and
        # security-group ingress are granted where the planner role is defined.
        return self.vpc.add_interface_endpoint(
            "SqsEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SQS,
            private_dns_enabled=True,
            open=False,
            security_groups=[self.sqs_endpoint_security_group],
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )

    def _grant_metadata_access(self, role: iam.Role) -> None:
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["rds-data:ExecuteStatement", "rds-data:BatchExecuteStatement"],
                resources=[self.metadata_cluster.cluster_arn],
            )
        )
        if self.metadata_cluster.secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        self.metadata_cluster.secret.grant_read(role)

    def _create_ingestion_function(
        self,
        construct_id: str,
        *,
        handler: str,
        role: iam.Role,
        source_format: str,
        reserved_concurrency: int,
    ) -> lambda_.Function:
        function_name = f"{self.stack_prefix}-{construct_id.lower()}"
        log_group = self._create_log_group(
            f"{construct_id}LogGroup",
            f"/aws/lambda/{function_name}",
        )
        log_group.grant_write(role)
        if self.metadata_cluster.secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        return lambda_.Function(
            self,
            f"{construct_id}Function",
            function_name=function_name,
            runtime=lambda_.Runtime.PYTHON_3_14,
            architecture=lambda_.Architecture.ARM_64,
            code=self._lambda_code(),
            handler=handler,
            role=role,
            description=f"Parses, indexes, and persists {source_format} document metadata",
            environment={
                "ERROR_BUCKET": self.error_bucket.bucket_name,
                "MAX_BULK_BYTES": str(5 * 1024 * 1024),
                "MAX_OBJECT_BYTES": str(50 * 1024 * 1024),
                "METADATA_CLUSTER_ARN": self.metadata_cluster.cluster_arn,
                "METADATA_DATABASE": METADATA_DATABASE_NAME,
                "METADATA_SECRET_ARN": self.metadata_cluster.secret.secret_arn,
                "METADATA_TABLE": METADATA_TABLE_NAME,
                "OPENSEARCH_ENDPOINT": self.search_collection.attr_collection_endpoint,
                "OPENSEARCH_SERVICE": "aoss",
                "OPENSEARCH_HL7_INDEX": "hl7-messages-v1",
                "OPENSEARCH_CCDA_INDEX": "ccda-documents-v1",
                "PARSED_BUCKET": self.parsed_bucket.bucket_name,
                "SOURCE_FORMAT": source_format,
            },
            environment_encryption=self.encryption_key,
            ephemeral_storage_size=Size.mebibytes(1024),
            log_group=log_group,
            memory_size=2048,
            reserved_concurrent_executions=reserved_concurrency,
            security_groups=[self.ingestion_security_group],
            timeout=Duration.minutes(10),
            tracing=lambda_.Tracing.ACTIVE,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )

    def _lambda_code(self) -> lambda_.Code:
        return lambda_.Code.from_asset(
            ".",
            bundling=BundlingOptions(
                image=lambda_.Runtime.PYTHON_3_14.bundling_image,
                local=cast(ILocalBundling, _LambdaBundler()),
                command=[
                    "bash",
                    "-c",
                    " && ".join(
                        [
                            "python -m pip install --disable-pip-version-check "
                            "--no-cache-dir --require-hashes "
                            "-r /asset-input/lambda-requirements.txt "
                            "--target /asset-output",
                            "cp -a /asset-input/src /asset-output/src",
                            "cp -a /asset-input/schema /asset-output/schema",
                            "rm -f /asset-output/src/stack.py /asset-output/src/config.py",
                            "find /asset-output -type d -name __pycache__ -prune -exec rm -rf {} +",
                        ]
                    ),
                ],
                platform="linux/arm64",
            ),
            # Development files and infrastructure modules never enter the runtime artifact.
            exclude=[
                ".git/**",
                ".mypy_cache/**",
                ".pytest_cache/**",
                ".ruff_cache/**",
                ".venv/**",
                "cdk.out/**",
                "tests/**",
                "tools/**",
                "web/**",
                "src/stack.py",
                "src/config.py",
                ".coverage",
            ],
        )

    def _create_log_group(self, construct_id: str, log_group_name: str) -> logs.LogGroup:
        return logs.LogGroup(
            self,
            construct_id,
            log_group_name=log_group_name,
            encryption_key=self.encryption_key,
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.RETAIN,
        )

    def _connect_event_sources(self) -> None:
        # Batch size one makes each acknowledgement correspond to one source S3 object.
        for function, queue, concurrency in (
            (self.hl7_function, self.hl7_queue, 10),
            (self.ccda_function, self.ccda_queue, 5),
        ):
            function.add_event_source(
                lambda_event_sources.SqsEventSource(
                    queue,
                    batch_size=1,
                    max_concurrency=concurrency,
                    report_batch_item_failures=True,
                )
            )

    def _create_operational_alarms(self) -> None:
        self._create_queue_alarms("Hl7", self.hl7_queue, self.hl7_dead_letter_queue)
        self._create_queue_alarms("Ccda", self.ccda_queue, self.ccda_dead_letter_queue)
        self._create_queue_alarms("Reindex", self.reindex_queue, self.reindex_dead_letter_queue)
        for construct_id, function in (
            ("Hl7", self.hl7_function),
            ("Ccda", self.ccda_function),
        ):
            cloudwatch.Alarm(
                self,
                f"{construct_id}FunctionErrorAlarm",
                alarm_description=f"{construct_id.upper()} Lambda reported an invocation error",
                metric=function.metric_errors(period=Duration.minutes(5), statistic="Sum"),
                threshold=1,
                evaluation_periods=1,
                datapoints_to_alarm=1,
                comparison_operator=(
                    cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
                ),
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            )

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
                f"{construct_id.upper()} queue has unprocessed messages older than 15 minutes"
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
            alarm_description=f"{construct_id.upper()} dead-letter queue contains failed messages",
            comparison_operator=(cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD),
            datapoints_to_alarm=1,
            evaluation_periods=1,
            metric=dead_letter_queue.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(5),
                statistic="Maximum",
            ),
            threshold=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

    def _search_collection_name(self) -> str:
        return f"manifest-medex-{self.config.environment.value}"

    def _create_explorer_security_group(self) -> ec2.SecurityGroup:
        return ec2.SecurityGroup(
            self,
            "ExplorerSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Network access for the authenticated clinical-message explorer Lambda",
        )

    def _create_explorer_role(self) -> iam.Role:
        role = iam.Role(
            self,
            "ExplorerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Execution role for the authenticated message explorer and SQL console",
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:UnassignPrivateIpAddresses",
                ],
                resources=["*"],
            )
        )
        # Read-only, version-aware access to stored clinical bodies in both data buckets only.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[
                    self.raw_bucket.arn_for_objects("*"),
                    self.parsed_bucket.arn_for_objects("*"),
                ],
            )
        )
        # Reports definitions now live in the catalog table; only generated outputs are read
        # from the reports bucket for authenticated download.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[self.reports_bucket.arn_for_objects(f"{REPORT_OUTPUT_PREFIX}*")],
            )
        )
        # The Reports API owns row-granular catalog CRUD on the single catalog table. Each
        # mutation pairs its change with an append-only audit item in one TransactWriteItems,
        # and GetItem backs single-row and history reads.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:TransactWriteItems",
                ],
                resources=[self.reports_catalog_table.table_arn],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                ],
                resources=[
                    self.report_runs_table.table_arn,
                    f"{self.report_runs_table.table_arn}/index/{REPORT_RUNS_INDEX_NAME}",
                ],
            )
        )
        # Reports S3 and DynamoDB CMK usage requires encrypt as well as decrypt.
        self.encryption_key.grant_encrypt_decrypt(role)
        NagSuppressions.add_resource_suppressions(
            role,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "Lambda VPC APIs require wildcard resources; S3 object permissions use "
                        "suffix wildcards within the raw and parsed data buckets and the reports "
                        "bucket outputs/ prefix; DynamoDB access targets the named catalog and "
                        "runs tables and the one named runs index; the runner invoke grant and "
                        "KMS APIs use service-defined wildcards on the single stack function and "
                        "key."
                    ),
                }
            ],
            apply_to_children=True,
        )
        return role

    def _authorize_explorer_data_api(self) -> None:
        # The explorer reaches Aurora only over the private Data API interface endpoint.
        self.explorer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["rds-data:ExecuteStatement"],
                resources=[self.metadata_cluster.cluster_arn],
            )
        )
        if self.metadata_cluster.secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        self.metadata_cluster.secret.grant_read(self.explorer_role)
        self.data_api_endpoint_security_group.add_ingress_rule(
            self.explorer_security_group,
            ec2.Port.tcp(443),
            "HTTPS from the explorer Lambda to the RDS Data API",
        )
        self.data_api_endpoint.add_to_policy(
            iam.PolicyStatement(
                principals=[self.explorer_role],
                actions=["rds-data:ExecuteStatement"],
                resources=[self.metadata_cluster.cluster_arn],
            )
        )

    def _create_report_runner_security_group(self) -> ec2.SecurityGroup:
        return ec2.SecurityGroup(
            self,
            "ReportRunnerSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Network access for the asynchronous report runner Lambda",
        )

    def _create_report_runner_role(self) -> iam.Role:
        role = iam.Role(
            self,
            "ReportRunnerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Least-privilege execution role for the asynchronous report runner",
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:UnassignPrivateIpAddresses",
                ],
                resources=["*"],
            )
        )
        # Definitions are read from the catalog table with a single partition Query; the
        # partitioned archive is written under the reports bucket outputs/ prefix.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[self.reports_catalog_table.table_arn],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[self.reports_bucket.arn_for_objects(f"{REPORT_OUTPUT_PREFIX}*")],
            )
        )
        # Run progress bookkeeping only updates and reads the runs table.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:UpdateItem", "dynamodb:GetItem"],
                resources=[self.report_runs_table.table_arn],
            )
        )
        self.encryption_key.grant_encrypt_decrypt(role)
        NagSuppressions.add_resource_suppressions(
            role,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "Lambda VPC APIs require wildcard resources; the S3 object permission "
                        "uses a suffix wildcard within the reports bucket outputs/ prefix; "
                        "DynamoDB access targets the named catalog and runs tables; KMS APIs use "
                        "service-defined wildcards on the single stack key."
                    ),
                }
            ],
            apply_to_children=True,
        )
        return role

    def _create_report_runner_function(self) -> lambda_.Function:
        function_name = f"{self.stack_prefix}-report-runner"
        log_group = self._create_log_group(
            "ReportRunnerLogGroup",
            f"/aws/lambda/{function_name}",
        )
        log_group.grant_write(self.report_runner_role)
        return lambda_.Function(
            self,
            "ReportRunnerFunction",
            function_name=function_name,
            runtime=lambda_.Runtime.PYTHON_3_14,
            architecture=lambda_.Architecture.ARM_64,
            code=self._lambda_code(),
            handler="src.report_runner.handler",
            role=self.report_runner_role,
            description="Runs one report definition and writes a partitioned CSV archive",
            environment={
                "REPORT_BUCKET": self.reports_bucket.bucket_name,
                "REPORT_CATALOG_TABLE": self.reports_catalog_table.table_name,
                "RUNS_TABLE": self.report_runs_table.table_name,
                "OPENSEARCH_ENDPOINT": self.search_collection.attr_collection_endpoint,
                "OPENSEARCH_SERVICE": "aoss",
                "OPENSEARCH_HL7_INDEX": HL7_INDEX_NAME,
                "OPENSEARCH_CCDA_INDEX": CCDA_INDEX_NAME,
            },
            environment_encryption=self.encryption_key,
            memory_size=1024,
            reserved_concurrent_executions=2,
            security_groups=[self.report_runner_security_group],
            timeout=Duration.minutes(15),
            tracing=lambda_.Tracing.ACTIVE,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )

    def _create_lambda_vpc_endpoint(self) -> ec2.InterfaceVpcEndpoint:
        # The explorer invokes the runner over a private Lambda endpoint; no NAT path exists.
        security_group = ec2.SecurityGroup(
            self,
            "LambdaEndpointSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=False,
            description="Allows HTTPS to the Lambda API only from the explorer",
        )
        security_group.add_ingress_rule(
            self.explorer_security_group,
            ec2.Port.tcp(443),
            "HTTPS from the explorer Lambda to the Lambda API",
        )
        return self.vpc.add_interface_endpoint(
            "LambdaEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.LAMBDA_,
            private_dns_enabled=True,
            open=False,
            security_groups=[security_group],
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )

    def _authorize_report_network(self) -> None:
        # The runner and explorer reach OpenSearch Serverless only through the private endpoint.
        self.serverless_endpoint_security_group.add_ingress_rule(
            self.report_runner_security_group,
            ec2.Port.tcp(443),
            "HTTPS from the report runner Lambda to OpenSearch Serverless",
        )
        self.serverless_endpoint_security_group.add_ingress_rule(
            self.explorer_security_group,
            ec2.Port.tcp(443),
            "HTTPS from the explorer Lambda to OpenSearch Serverless",
        )
        self.lambda_vpc_endpoint = self._create_lambda_vpc_endpoint()
        # The explorer is the only principal permitted to dispatch report runs.
        self.report_runner_function.grant_invoke(self.explorer_role)

    def _create_reingest_planner_security_group(self) -> ec2.SecurityGroup:
        return ec2.SecurityGroup(
            self,
            "ReingestPlannerSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Network access for the reingestion planner Lambda",
        )

    def _create_reindexer_security_group(self) -> ec2.SecurityGroup:
        return ec2.SecurityGroup(
            self,
            "ReindexerSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Network access for the parsed-zone reindexer Lambda",
        )

    def _create_reingest_planner_role(self) -> iam.Role:
        role = iam.Role(
            self,
            "ReingestPlannerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Least-privilege execution role for the reingestion planner",
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:UnassignPrivateIpAddresses",
                ],
                resources=["*"],
            )
        )
        # The planner drives the job lifecycle and fans documents out to the reindex queue.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:UpdateItem", "dynamodb:GetItem"],
                resources=[self.reingest_jobs_table.table_arn],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["sqs:SendMessage"],
                resources=[self.reindex_queue.queue_arn],
            )
        )
        # Sending to the KMS-encrypted queue and reading the CMK-encrypted Aurora secret
        # both require key usage; DynamoDB encryption at rest is served by its own grant.
        self.encryption_key.grant_encrypt_decrypt(role)
        NagSuppressions.add_resource_suppressions(
            role,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "Lambda VPC APIs require wildcard resources; DynamoDB and SQS access "
                        "target the named jobs table and reindex queue; KMS APIs use "
                        "service-defined wildcards on the single stack key."
                    ),
                }
            ],
            apply_to_children=True,
        )
        return role

    def _create_reindexer_role(self) -> iam.Role:
        role = iam.Role(
            self,
            "ReindexerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Least-privilege execution role for the parsed-zone reindexer",
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:UnassignPrivateIpAddresses",
                ],
                resources=["*"],
            )
        )
        # The reindexer only reads already-parsed objects; it never touches raw source.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[self.parsed_bucket.arn_for_objects("*")],
            )
        )
        # Per-document outcome counters are recorded with atomic ADD updates only.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:UpdateItem"],
                resources=[self.reingest_jobs_table.table_arn],
            )
        )
        # Reading the SSE-KMS parsed object and consuming the KMS-encrypted queue both need
        # only Decrypt; DynamoDB encryption at rest is served by its own service grant.
        self.encryption_key.grant_decrypt(role)
        NagSuppressions.add_resource_suppressions(
            role,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "Lambda VPC APIs require wildcard resources; the S3 object permission "
                        "uses a suffix wildcard within the parsed data bucket; KMS APIs use "
                        "service-defined wildcards on the single stack key."
                    ),
                }
            ],
            apply_to_children=True,
        )
        return role

    def _authorize_reingest_planner_data_api(self) -> None:
        # The planner resolves parsed-zone locations over the private Data API endpoint only.
        self.reingest_planner_role.add_to_policy(
            iam.PolicyStatement(
                actions=["rds-data:ExecuteStatement"],
                resources=[self.metadata_cluster.cluster_arn],
            )
        )
        if self.metadata_cluster.secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        self.metadata_cluster.secret.grant_read(self.reingest_planner_role)
        self.data_api_endpoint_security_group.add_ingress_rule(
            self.reingest_planner_security_group,
            ec2.Port.tcp(443),
            "HTTPS from the reingestion planner Lambda to the RDS Data API",
        )
        self.data_api_endpoint.add_to_policy(
            iam.PolicyStatement(
                principals=[self.reingest_planner_role],
                actions=["rds-data:ExecuteStatement"],
                resources=[self.metadata_cluster.cluster_arn],
            )
        )
        self.sqs_endpoint_security_group.add_ingress_rule(
            self.reingest_planner_security_group,
            ec2.Port.tcp(443),
            "HTTPS from the reingestion planner Lambda to Amazon SQS",
        )
        self.sqs_endpoint.add_to_policy(
            iam.PolicyStatement(
                principals=[self.reingest_planner_role],
                actions=["sqs:SendMessage"],
                resources=[self.reindex_queue.queue_arn],
            )
        )

    def _authorize_reindexer_network(self) -> None:
        # The reindexer reaches OpenSearch Serverless only through the private endpoint.
        self.serverless_endpoint_security_group.add_ingress_rule(
            self.reindexer_security_group,
            ec2.Port.tcp(443),
            "HTTPS from the reindexer Lambda to OpenSearch Serverless",
        )

    def _create_reingest_planner_function(self) -> lambda_.Function:
        function_name = f"{self.stack_prefix}-reingest-planner"
        log_group = self._create_log_group(
            "ReingestPlannerLogGroup",
            f"/aws/lambda/{function_name}",
        )
        log_group.grant_write(self.reingest_planner_role)
        if self.metadata_cluster.secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        return lambda_.Function(
            self,
            "ReingestPlannerFunction",
            function_name=function_name,
            runtime=lambda_.Runtime.PYTHON_3_14,
            architecture=lambda_.Architecture.ARM_64,
            code=self._lambda_code(),
            handler="src.reingest_planner.handler",
            role=self.reingest_planner_role,
            description="Resolves a reingestion job's documents and enqueues them for reindexing",
            environment={
                "JOBS_TABLE": self.reingest_jobs_table.table_name,
                "METADATA_CLUSTER_ARN": self.metadata_cluster.cluster_arn,
                "METADATA_DATABASE": METADATA_DATABASE_NAME,
                "METADATA_SECRET_ARN": self.metadata_cluster.secret.secret_arn,
                "METADATA_TABLE": METADATA_TABLE_NAME,
                "REINDEX_QUEUE_URL": self.reindex_queue.queue_url,
            },
            environment_encryption=self.encryption_key,
            memory_size=1024,
            reserved_concurrent_executions=1,
            security_groups=[self.reingest_planner_security_group],
            timeout=Duration.minutes(15),
            tracing=lambda_.Tracing.ACTIVE,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )

    def _create_reindexer_function(self) -> lambda_.Function:
        function_name = f"{self.stack_prefix}-reindexer"
        log_group = self._create_log_group(
            "ReindexerLogGroup",
            f"/aws/lambda/{function_name}",
        )
        log_group.grant_write(self.reindexer_role)
        return lambda_.Function(
            self,
            "ReindexerFunction",
            function_name=function_name,
            runtime=lambda_.Runtime.PYTHON_3_14,
            architecture=lambda_.Architecture.ARM_64,
            code=self._lambda_code(),
            handler="src.reindexer_handler.handler",
            role=self.reindexer_role,
            description="Re-indexes already-parsed clinical documents for a reingestion job",
            environment={
                "PARSED_BUCKET": self.parsed_bucket.bucket_name,
                "JOBS_TABLE": self.reingest_jobs_table.table_name,
                "OPENSEARCH_ENDPOINT": self.search_collection.attr_collection_endpoint,
                "OPENSEARCH_SERVICE": "aoss",
                "OPENSEARCH_HL7_INDEX": HL7_INDEX_NAME,
                "OPENSEARCH_CCDA_INDEX": CCDA_INDEX_NAME,
                "MAX_RECEIVE_COUNT": "5",
                # Parser versions are resolved at synth from the parser modules so the
                # reindexer can flag stale parses without importing a parser at runtime.
                "HL7_PARSER_VERSION": HL7_PARSER_VERSION,
                "CCDA_PARSER_VERSION": CCDA_PARSER_VERSION,
            },
            environment_encryption=self.encryption_key,
            memory_size=1024,
            reserved_concurrent_executions=5,
            security_groups=[self.reindexer_security_group],
            timeout=Duration.minutes(5),
            tracing=lambda_.Tracing.ACTIVE,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )

    def _connect_reindex_event_source(self) -> None:
        # The reindexer consumes the reindex queue in bounded batches with partial-batch
        # failure reporting; SqsEventSource generates the queue consume grants on its role.
        self.reindexer_function.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.reindex_queue,
                batch_size=10,
                max_concurrency=5,
                report_batch_item_failures=True,
            )
        )

    def _authorize_explorer_reingest(self) -> None:
        # The explorer owns the reingestion jobs API: it previews, creates, lists, and reads
        # job rows and dispatches the planner over the existing private Lambda endpoint.
        self.explorer_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:Scan",
                ],
                resources=[self.reingest_jobs_table.table_arn],
            )
        )
        self.reingest_planner_function.grant_invoke(self.explorer_role)

    def _create_explorer_function(self) -> lambda_.Function:
        function_name = f"{self.stack_prefix}-explorer"
        log_group = self._create_log_group(
            "ExplorerLogGroup",
            f"/aws/lambda/{function_name}",
        )
        log_group.grant_write(self.explorer_role)
        if self.metadata_cluster.secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        return lambda_.Function(
            self,
            "ExplorerFunction",
            function_name=function_name,
            runtime=lambda_.Runtime.PYTHON_3_14,
            architecture=lambda_.Architecture.ARM_64,
            code=self._lambda_code(),
            handler="src.explorer_handler.handler",
            role=self.explorer_role,
            description="Serves authenticated message exploration and SQL execution",
            environment={
                "MAX_BODY_BYTES": str(MAX_EXPLORER_BODY_BYTES),
                "METADATA_CLUSTER_ARN": self.metadata_cluster.cluster_arn,
                "METADATA_DATABASE": METADATA_DATABASE_NAME,
                "METADATA_SECRET_ARN": self.metadata_cluster.secret.secret_arn,
                "METADATA_TABLE": METADATA_TABLE_NAME,
                "PARSED_BUCKET": self.parsed_bucket.bucket_name,
                "RAW_BUCKET": self.raw_bucket.bucket_name,
                "REPORT_BUCKET": self.reports_bucket.bucket_name,
                "REPORT_CATALOG_TABLE": self.reports_catalog_table.table_name,
                "REPORT_RUNS_TABLE": self.report_runs_table.table_name,
                "REPORT_RUNS_INDEX": REPORT_RUNS_INDEX_NAME,
                "REPORT_RUNNER_FUNCTION": self.report_runner_function.function_name,
                "REINGEST_JOBS_TABLE": self.reingest_jobs_table.table_name,
                "REINGEST_PLANNER_FUNCTION": self.reingest_planner_function.function_name,
                "OPENSEARCH_ENDPOINT": self.search_collection.attr_collection_endpoint,
                "OPENSEARCH_SERVICE": "aoss",
                "OPENSEARCH_HL7_INDEX": HL7_INDEX_NAME,
                "OPENSEARCH_CCDA_INDEX": CCDA_INDEX_NAME,
            },
            environment_encryption=self.encryption_key,
            memory_size=1024,
            reserved_concurrent_executions=10,
            security_groups=[self.explorer_security_group],
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
        )

    def _create_http_api(self) -> apigwv2.HttpApi:
        # Routes and their Cognito authorizer bind to the api after the frontend origin exists;
        # the named auto-deploy stage supplies the "/api" URL prefix consumed by CloudFront.
        return apigwv2.HttpApi(
            self,
            "ExplorerHttpApi",
            api_name=f"{self.stack_prefix}-explorer",
            description="Authenticated clinical-message explorer and SQL execution API",
            create_default_stage=False,
        )

    def _create_frontend_bucket(self) -> s3.Bucket:
        return s3.Bucket(
            self,
            "FrontendBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            minimum_tls_version=1.2,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix="frontend/",
            versioned=True,
        )

    def _create_distribution(self) -> cloudfront.Distribution:
        api_host = f"{self.http_api.api_id}.execute-api.{self.region}.{self.url_suffix}"
        security_headers = cloudfront.ResponseHeadersPolicy(
            self,
            "FrontendSecurityHeaders",
            comment="HSTS, nosniff, and frame-deny headers for the explorer frontend",
            security_headers_behavior=cloudfront.ResponseSecurityHeadersBehavior(
                content_type_options=cloudfront.ResponseHeadersContentTypeOptions(override=True),
                frame_options=cloudfront.ResponseHeadersFrameOptions(
                    frame_option=cloudfront.HeadersFrameOption.DENY,
                    override=True,
                ),
                strict_transport_security=cloudfront.ResponseHeadersStrictTransportSecurity(
                    access_control_max_age=Duration.days(365),
                    include_subdomains=True,
                    preload=True,
                    override=True,
                ),
            ),
        )
        # The API behavior forwards every header except Host so execute-api can resolve the
        # stage, disables caching for authenticated responses, and never persists request URIs.
        api_behavior = cloudfront.BehaviorOptions(
            origin=cloudfront_origins.HttpOrigin(
                api_host,
                protocol_policy=cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
            ),
            allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
            cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
            origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
            response_headers_policy=security_headers,
            viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        )
        distribution = cloudfront.Distribution(
            self,
            "FrontendDistribution",
            comment=f"{self.stack_prefix} clinical-message explorer frontend",
            default_root_object="index.html",
            minimum_protocol_version=cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
            default_behavior=cloudfront.BehaviorOptions(
                origin=cloudfront_origins.S3BucketOrigin.with_origin_access_control(
                    self.frontend_bucket
                ),
                allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
                response_headers_policy=security_headers,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
            ),
            additional_behaviors={"/api/*": api_behavior},
        )
        NagSuppressions.add_resource_suppressions(
            distribution,
            [
                {
                    "id": "AwsSolutions-CFR1",
                    "reason": (
                        "The explorer is a private, authenticated tool; access is controlled by "
                        "the Cognito user-pool authorizer rather than CloudFront geo filtering."
                    ),
                },
                {
                    "id": "AwsSolutions-CFR2",
                    "reason": (
                        "No AWS WAF web ACL is attached to this demo distribution; every API "
                        "route requires a valid Cognito user-pool token and the S3 origin is "
                        "reachable only through Origin Access Control."
                    ),
                },
                {
                    "id": "AwsSolutions-CFR3",
                    "reason": (
                        "CloudFront access logging is intentionally disabled to avoid persisting "
                        "request URIs that embed clinical document identifiers; S3 server access "
                        "logs and VPC flow logs provide the retained audit trail."
                    ),
                },
                {
                    "id": "AwsSolutions-CFR4",
                    "reason": (
                        "The distribution serves the default CloudFront domain and certificate, "
                        "whose minimum viewer TLS version cannot be raised without a custom "
                        "certificate; TLS 1.2 is enforced wherever configurable."
                    ),
                },
            ],
        )
        return distribution

    def _create_user_pool(
        self,
    ) -> tuple[cognito.UserPool, cognito.UserPoolClient, cognito.UserPoolDomain]:
        user_pool = cognito.UserPool(
            self,
            "ExplorerUserPool",
            user_pool_name=f"{self.stack_prefix}-explorer",
            self_sign_up_enabled=False,
            sign_in_aliases=cognito.SignInAliases(email=True),
            sign_in_case_sensitive=False,
            standard_attributes=cognito.StandardAttributes(
                email=cognito.StandardAttribute(required=True, mutable=True)
            ),
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            mfa=cognito.Mfa.OPTIONAL,
            mfa_second_factor=cognito.MfaSecondFactor(otp=True, sms=False),
            password_policy=cognito.PasswordPolicy(
                min_length=14,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=True,
            ),
            feature_plan=cognito.FeaturePlan.PLUS,
            standard_threat_protection_mode=cognito.StandardThreatProtectionMode.FULL_FUNCTION,
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            removal_policy=RemovalPolicy.RETAIN,
        )
        NagSuppressions.add_resource_suppressions(
            user_pool,
            [
                {
                    "id": "AwsSolutions-COG2",
                    "reason": (
                        "Multi-factor authentication is offered as optional software TOTP by "
                        "design so operators can enroll authenticator apps; SMS MFA is disabled "
                        "and advanced security protection is enforced."
                    ),
                }
            ],
        )
        domain = user_pool.add_domain(
            "ExplorerUserPoolDomain",
            cognito_domain=cognito.CognitoDomainOptions(
                domain_prefix=f"{self.stack_prefix}-{Aws.ACCOUNT_ID}"
            ),
        )
        callback_url = f"https://{self.distribution.distribution_domain_name}/"
        client = user_pool.add_client(
            "ExplorerUserPoolClient",
            user_pool_client_name=f"{self.stack_prefix}-explorer-web",
            generate_secret=False,
            auth_flows=cognito.AuthFlow(user_srp=True),
            prevent_user_existence_errors=True,
            o_auth=cognito.OAuthSettings(
                flows=cognito.OAuthFlows(authorization_code_grant=True),
                scopes=[
                    cognito.OAuthScope.OPENID,
                    cognito.OAuthScope.EMAIL,
                    cognito.OAuthScope.PROFILE,
                ],
                callback_urls=[callback_url],
                logout_urls=[callback_url],
            ),
            supported_identity_providers=[cognito.UserPoolClientIdentityProvider.COGNITO],
        )
        return user_pool, client, domain

    def _configure_explorer_routes(self) -> None:
        authorizer = apigwv2_authorizers.HttpUserPoolAuthorizer(
            "ExplorerUserPoolAuthorizer",
            self.user_pool,
            user_pool_clients=[self.user_pool_client],
            identity_source=["$request.header.Authorization"],
        )
        integration = apigwv2_integrations.HttpLambdaIntegration(
            "ExplorerIntegration",
            self.explorer_function,
        )
        routes: tuple[tuple[str, apigwv2.HttpMethod], ...] = (
            ("/messages", apigwv2.HttpMethod.GET),
            ("/messages/{documentId}", apigwv2.HttpMethod.GET),
            ("/messages/{documentId}/body", apigwv2.HttpMethod.POST),
            ("/query", apigwv2.HttpMethod.POST),
            ("/query-test", apigwv2.HttpMethod.POST),
            ("/search", apigwv2.HttpMethod.POST),
            ("/search/fields", apigwv2.HttpMethod.GET),
            ("/facilities", apigwv2.HttpMethod.GET),
            ("/reports", apigwv2.HttpMethod.GET),
            ("/reports/import", apigwv2.HttpMethod.POST),
            ("/reports/{id}", apigwv2.HttpMethod.GET),
            ("/reports/{id}", apigwv2.HttpMethod.PUT),
            ("/reports/{id}", apigwv2.HttpMethod.DELETE),
            ("/reports/{id}/history", apigwv2.HttpMethod.GET),
            ("/reports/{id}/export", apigwv2.HttpMethod.GET),
            ("/reports/{id}/sections", apigwv2.HttpMethod.POST),
            ("/reports/{id}/sections/{sseq}/rows", apigwv2.HttpMethod.POST),
            ("/reports/{id}/sections/{sseq}/rows/{rseq}", apigwv2.HttpMethod.PUT),
            ("/reports/{id}/sections/{sseq}/rows/{rseq}", apigwv2.HttpMethod.DELETE),
            ("/reports/{id}/runs", apigwv2.HttpMethod.GET),
            ("/reports/{id}/runs", apigwv2.HttpMethod.POST),
            ("/runs/{runId}", apigwv2.HttpMethod.GET),
            ("/runs/{runId}/download", apigwv2.HttpMethod.GET),
            ("/reingest/preview", apigwv2.HttpMethod.POST),
            ("/reingest/jobs", apigwv2.HttpMethod.POST),
            ("/reingest/jobs", apigwv2.HttpMethod.GET),
            ("/reingest/jobs/{jobId}", apigwv2.HttpMethod.GET),
        )
        for path, method in routes:
            self.http_api.add_routes(
                path=path,
                methods=[method],
                integration=integration,
                authorizer=authorizer,
            )

    def _create_http_stage(self) -> apigwv2.HttpStage:
        # An explicit auto-deploy stage named "api" supplies the URL path prefix while access
        # logging stays disabled so clinical document identifiers never reach request logs.
        stage = apigwv2.HttpStage(
            self,
            "ExplorerApiStage",
            http_api=self.http_api,
            stage_name="api",
            auto_deploy=True,
        )
        NagSuppressions.add_resource_suppressions(
            stage,
            [
                {
                    "id": "AwsSolutions-APIG1",
                    "reason": (
                        "Stage access logging is intentionally disabled: both message routes "
                        "embed the clinical document identifier in the request path, and "
                        "$context.path would persist it. PHI-access auditing is emitted from the "
                        "explorer Lambda without request URIs, and the private origin is only "
                        "reachable behind the Cognito user-pool authorizer."
                    ),
                }
            ],
        )
        return stage

    def _deploy_frontend(self) -> None:
        dist_path = _LambdaBundler._PROJECT_ROOT / "web" / "dist"
        if not dist_path.is_dir():
            raise RuntimeError(FRONTEND_DIST_MISSING)
        authority = (
            f"https://cognito-idp.{self.region}.{self.url_suffix}/{self.user_pool.user_pool_id}"
        )
        origin_url = f"https://{self.distribution.distribution_domain_name}/"
        runtime_config = {
            "apiBasePath": "/api",
            "authority": authority,
            "clientId": self.user_pool_client.user_pool_client_id,
            "postLogoutRedirectUri": origin_url,
            "redirectUri": origin_url,
        }
        s3_deployment.BucketDeployment(
            self,
            "FrontendDeployment",
            sources=[
                s3_deployment.Source.asset(str(dist_path)),
                s3_deployment.Source.json_data("config.json", runtime_config),
            ],
            destination_bucket=self.frontend_bucket,
            distribution=self.distribution,
            distribution_paths=["/*"],
            prune=True,
            retain_on_delete=False,
        )
        self._suppress_bucket_deployment()

    def _suppress_bucket_deployment(self) -> None:
        # The CDK-managed BucketDeployment handler is a shared construct outside our control.
        suppressions = [
            {
                "id": "AwsSolutions-IAM4",
                "reason": (
                    "The CDK-managed BucketDeployment handler attaches the AWS-managed basic "
                    "Lambda execution policy; this construct is not authored by this stack."
                ),
            },
            {
                "id": "AwsSolutions-IAM5",
                "reason": (
                    "The CDK-managed BucketDeployment handler requires wildcard read access to "
                    "the asset bucket and write access to the destination bucket it provisions."
                ),
            },
            {
                "id": "AwsSolutions-L1",
                "reason": (
                    "The BucketDeployment handler runtime is pinned by the CDK library version "
                    "and cannot be selected by this stack."
                ),
            },
        ]
        for child in self.node.children:
            if child.node.id.startswith("Custom::CDKBucketDeployment"):
                NagSuppressions.add_resource_suppressions(
                    child,
                    suppressions,
                    apply_to_children=True,
                )

    def _create_outputs(self) -> None:
        if self.metadata_cluster.secret is None:
            raise RuntimeError(AURORA_CREDENTIALS_MISSING)
        outputs = {
            "EncryptionKeyArn": self.encryption_key.key_arn,
            "RawBucketName": self.raw_bucket.bucket_name,
            "ParsedBucketName": self.parsed_bucket.bucket_name,
            "ErrorBucketName": self.error_bucket.bucket_name,
            "Hl7QueueUrl": self.hl7_queue.queue_url,
            "CcdaQueueUrl": self.ccda_queue.queue_url,
            "Hl7FunctionName": self.hl7_function.function_name,
            "CcdaFunctionName": self.ccda_function.function_name,
            "OpenSearchEndpoint": self.search_collection.attr_collection_endpoint,
            "OpenSearchCollectionArn": self.search_collection.attr_arn,
            "OpenSearchCollectionName": self.search_collection.name,
            "OpenSearchHl7Index": "hl7-messages-v1",
            "OpenSearchCcdaIndex": "ccda-documents-v1",
            "AuroraClusterArn": self.metadata_cluster.cluster_arn,
            "AuroraClusterIdentifier": self.metadata_cluster.cluster_identifier,
            "AuroraSecretArn": self.metadata_cluster.secret.secret_arn,
            "MetadataDatabaseName": METADATA_DATABASE_NAME,
            "MetadataTableName": METADATA_TABLE_NAME,
            "ExplorerFunctionName": self.explorer_function.function_name,
            "ExplorerApiEndpoint": self.http_api.api_endpoint,
            "ExplorerApiStageName": self.http_stage.stage_name,
            "ReportsBucketName": self.reports_bucket.bucket_name,
            "ReportsCatalogTableName": self.reports_catalog_table.table_name,
            "ReportRunsTableName": self.report_runs_table.table_name,
            "ReportRunnerFunctionName": self.report_runner_function.function_name,
            "ReindexQueueUrl": self.reindex_queue.queue_url,
            "ReingestJobsTableName": self.reingest_jobs_table.table_name,
            "ReingestPlannerFunctionName": self.reingest_planner_function.function_name,
            "ReindexerFunctionName": self.reindexer_function.function_name,
            "UserPoolId": self.user_pool.user_pool_id,
            "UserPoolClientId": self.user_pool_client.user_pool_client_id,
            "UserPoolHostedUiDomain": self.user_pool_domain.domain_name,
            "FrontendBucketName": self.frontend_bucket.bucket_name,
            "FrontendDistributionId": self.distribution.distribution_id,
            "FrontendDistributionDomainName": self.distribution.distribution_domain_name,
        }
        for output_id, value in outputs.items():
            CfnOutput(self, output_id, value=value)
