"""Define the private, format-specific AWS ingestion platform with CDK."""

from importlib import metadata
from pathlib import Path
from shutil import copy2, copytree, ignore_patterns
from typing import Any, ClassVar, cast

import jsii
from aws_cdk import (
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
from aws_cdk import aws_cloudwatch as cloudwatch
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
from aws_cdk import aws_sqs as sqs
from cdk_nag import NagSuppressions
from constructs import Construct
from src.config import AppConfig, DeploymentEnvironment

METADATA_DATABASE_NAME = "manifest_medex"
METADATA_TABLE_NAME = "document_metadata"
AURORA_CREDENTIALS_MISSING = "Aurora generated credentials did not produce a secret"


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

        self.hl7_role = self._create_ingestion_role("Hl7", "incoming/hl7/*", "hl7/*")
        self.ccda_role = self._create_ingestion_role("Ccda", "incoming/ccda/*", "ccda/*")

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
        self, construct_id: str, service_name: str
    ) -> ec2.SecurityGroup:
        security_group = ec2.SecurityGroup(
            self,
            construct_id,
            vpc=self.vpc,
            allow_all_outbound=False,
            description=f"Allows HTTPS to {service_name} only from ingestion Lambdas",
        )
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
        for role in (self.hl7_role, self.ccda_role):
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
        }
        for output_id, value in outputs.items():
            CfnOutput(self, output_id, value=value)
