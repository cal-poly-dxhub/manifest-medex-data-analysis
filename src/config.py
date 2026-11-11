"""Validate deployment context and enforce environment-specific safety constraints."""

import os
import re
from enum import StrEnum
from typing import Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from constructs import Node
from pydantic import BaseModel, ConfigDict, Field, model_validator


def _context_bool(value: object, *, default: bool) -> bool:
    """Parse CDK boolean context values without treating the string 'false' as true."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes"}:
            return True
        if normalized in {"0", "false", "no"}:
            return False
    msg = f"Invalid boolean context value: {value!r}"
    raise ValueError(msg)


_GATEWAY_SERVICES = frozenset({"s3", "dynamodb"})
# ecr.api, ecr.dkr, and logs let the ECS ingestion tasks pull their image and write logs.
_INTERFACE_SERVICES = frozenset({"rds-data", "sqs", "lambda", "ecr.api", "ecr.dkr", "logs"})
_ENDPOINT_SERVICES = _GATEWAY_SERVICES | _INTERFACE_SERVICES


def _context_int(value: object, *, default: int) -> int:
    """Parse an integer CDK context value given as a number or a string."""
    if value is None or value == "":
        return default
    # bool is an int subclass; reject it so "true" never becomes 1.
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and re.fullmatch(r"\s*-?\d+\s*", value):
        return int(value)
    msg = f"Invalid integer context value: {value!r}"
    raise ValueError(msg)


def _context_float(value: object, *, default: float) -> float:
    """Parse a numeric CDK context value given as a number or a string."""
    if value is None or value == "":
        return default
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
    msg = f"Invalid numeric context value: {value!r}"
    raise ValueError(msg)


# Fargate supports these memory sizes (MiB) for the 1 and 2 vCPU task sizes this stack uses.
_FARGATE_MEMORY_BY_CPU = {
    1024: frozenset(range(2048, 8193, 1024)),
    2048: frozenset(range(4096, 16385, 1024)),
}


class IngestionLaneConfig(BaseModel):
    """Sizing and scaling for one format's ECS ingestion service."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    task_cpu: int
    task_memory: int
    workers: int = Field(ge=1, le=32)
    receive_batch_size: int = Field(ge=1, le=10)
    min_tasks: int = Field(ge=1, le=50)
    max_tasks: int = Field(ge=1, le=50)
    # Target tracking keeps (visible messages / running tasks) near this value. Derived from
    # measured per-task throughput x acceptable queue delay (about 1-2 minutes).
    backlog_target: int = Field(ge=1, le=100_000)

    @model_validator(mode="after")
    def require_valid_fargate_sizing(self) -> Self:
        allowed = _FARGATE_MEMORY_BY_CPU.get(self.task_cpu)
        if allowed is None:
            msg = f"task_cpu must be one of {sorted(_FARGATE_MEMORY_BY_CPU)}"
            raise ValueError(msg)
        if self.task_memory not in allowed:
            msg = f"task_memory {self.task_memory} is not valid for task_cpu {self.task_cpu}"
            raise ValueError(msg)
        if self.min_tasks > self.max_tasks:
            msg = "min_tasks must not exceed max_tasks"
            raise ValueError(msg)
        return self


# Production-sized defaults, measured on MX-sized test data (see cost_sim results).
HL7_LANE_DEFAULTS = IngestionLaneConfig(
    task_cpu=1024,
    task_memory=2048,
    workers=8,
    receive_batch_size=10,
    min_tasks=2,
    max_tasks=10,
    backlog_target=1000,
)
CCDA_LANE_DEFAULTS = IngestionLaneConfig(
    task_cpu=1024,
    task_memory=4096,
    workers=4,
    receive_batch_size=1,
    min_tasks=2,
    max_tasks=10,
    backlog_target=200,
)
_LANE_KEYS = (
    "task_cpu",
    "task_memory",
    "workers",
    "receive_batch_size",
    "min_tasks",
    "max_tasks",
    "backlog_target",
)


def _lane_from_context(
    node: Node, prefix: str, defaults: IngestionLaneConfig
) -> IngestionLaneConfig:
    values = {
        key: _context_int(node.try_get_context(f"{prefix}_{key}"), default=getattr(defaults, key))
        for key in _LANE_KEYS
    }
    return IngestionLaneConfig(**values)


def _context_list(value: object) -> tuple[str, ...]:
    """Parse a CDK context list given as JSON list or comma-separated string."""
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    msg = f"Invalid list context value: {value!r}"
    raise ValueError(msg)


class DeploymentEnvironment(StrEnum):
    """Supported deployment stages."""

    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class AppConfig(BaseModel):
    """Validated configuration supplied through CDK context or environment variables."""

    # Strict, immutable configuration prevents silent coercion or post-validation drift.
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    project_name: str = Field(pattern=r"^[a-z][a-z0-9-]{2,62}$")
    environment: DeploymentEnvironment
    account: str | None = Field(default=None, pattern=r"^\d{12}$")
    region: str | None = Field(default=None, pattern=r"^[a-z]{2}(?:-gov)?-[a-z]+-\d$")
    enable_cdk_nag: bool = True
    enable_public_dashboard: bool = False
    dashboard_principal_arn: str | None = Field(
        default=None,
        pattern=r"^arn:(?:aws|aws-us-gov|aws-cn):iam::\d{12}:role/.+$",
    )
    termination_protection: bool = False
    # Optional: deploy into a customer-provided VPC instead of creating one. All four must
    # be supplied together. Subnets must be private (no route to an internet gateway), span
    # at least two Availability Zones, and have free IP space for endpoint and Lambda ENIs.
    # The stack still creates every VPC endpoint it needs inside these subnets.
    vpc_id: str | None = Field(default=None, pattern=r"^vpc-[0-9a-f]{8,17}$")
    subnet_ids: tuple[str, ...] = ()
    route_table_ids: tuple[str, ...] = ()
    availability_zones: tuple[str, ...] = ()
    # Optional: VPC endpoints that already exist in the customer VPC and must not be
    # created again (a second interface endpoint for a service with private DNS fails).
    # Gateway services (s3, dynamodb) need no further detail. Interface services need the
    # endpoint id and its security group id so the stack can import the endpoint and allow
    # our Lambdas through its security group. Format per entry:
    #   "s3" | "dynamodb" | "rds-data=vpce-...:sg-..." | "sqs=vpce-...:sg-..."
    #   | "lambda=vpce-...:sg-..."
    existing_endpoints: tuple[str, ...] = ()
    # ECS ingestion: the worker image digest is produced by deploy.sh (CodeBuild) and is
    # required for a deployment; local synthesis and tests use a placeholder image.
    worker_image_digest: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    hl7: IngestionLaneConfig = HL7_LANE_DEFAULTS
    ccda: IngestionLaneConfig = CCDA_LANE_DEFAULTS
    # Optional scheduled scaling: raise both services' minimum task count on weekdays
    # 07:45-17:30 in work_hours_timezone so capacity is ready before the morning ramp.
    work_hours_min_tasks: int | None = Field(default=None, ge=1, le=50)
    work_hours_timezone: str = Field(
        default="America/Los_Angeles", pattern=r"^[A-Za-z_]+/[A-Za-z_]+$"
    )
    # Aurora Serverless v2 capacity range (ACUs). Serverless v2 bills only the capacity in
    # use, so max_acu is a ceiling against runaway cost, not a standing charge.
    aurora_min_acu: float = Field(default=2.0, ge=0.5, le=256)
    aurora_max_acu: float = Field(default=16.0, ge=1, le=256)

    @property
    def uses_existing_vpc(self) -> bool:
        return self.vpc_id is not None

    @property
    def existing_endpoint_map(self) -> dict[str, tuple[str, str] | None]:
        """Parse ``existing_endpoints`` into {service: (endpoint_id, sg_id) | None}."""
        parsed: dict[str, tuple[str, str] | None] = {}
        for entry in self.existing_endpoints:
            service, _, detail = entry.partition("=")
            if detail:
                endpoint_id, _, sg_id = detail.partition(":")
                parsed[service] = (endpoint_id, sg_id)
            else:
                parsed[service] = None
        return parsed

    @model_validator(mode="after")
    def require_safe_production_configuration(self) -> Self:
        """Reject a production deployment that disables termination protection."""
        if self.environment is DeploymentEnvironment.PROD and not self.termination_protection:
            msg = "Production configuration requires termination protection"
            raise ValueError(msg)
        # Account and region form one CDK environment and must be supplied atomically.
        if (self.account is None) != (self.region is None):
            msg = "CDK account and region must be provided together"
            raise ValueError(msg)
        # Public browser access is a development-only exception for one explicit role.
        if self.enable_public_dashboard:
            if self.environment is not DeploymentEnvironment.DEV:
                msg = "Public Dashboard access is allowed only in development"
                raise ValueError(msg)
            if self.dashboard_principal_arn is None:
                msg = "Public Dashboard access requires dashboard_principal_arn"
                raise ValueError(msg)
        elif self.dashboard_principal_arn is not None:
            msg = "dashboard_principal_arn requires enable_public_dashboard=true"
            raise ValueError(msg)
        existing = (self.vpc_id, self.subnet_ids, self.route_table_ids, self.availability_zones)
        supplied = [bool(value) for value in existing]
        if any(supplied) and not all(supplied):
            msg = (
                "Existing-VPC deployment requires vpc_id, subnet_ids, route_table_ids, and "
                "availability_zones together"
            )
            raise ValueError(msg)
        if self.uses_existing_vpc:
            if len(self.subnet_ids) < 2:
                msg = "Existing-VPC deployment requires at least two subnets (Aurora needs two AZs)"
                raise ValueError(msg)
            if len(self.route_table_ids) != len(self.subnet_ids):
                msg = "route_table_ids must list one route table per subnet, in the same order"
                raise ValueError(msg)
            if len(self.availability_zones) != len(self.subnet_ids):
                msg = "availability_zones must list one AZ per subnet, in the same order"
                raise ValueError(msg)
            if len(set(self.availability_zones)) < 2:
                msg = "Existing-VPC subnets must span at least two Availability Zones"
                raise ValueError(msg)
            for subnet_id in self.subnet_ids:
                if not re.fullmatch(r"subnet-[0-9a-f]{8,17}", subnet_id):
                    msg = f"Invalid subnet id: {subnet_id!r}"
                    raise ValueError(msg)
            for route_table_id in self.route_table_ids:
                if not re.fullmatch(r"rtb-[0-9a-f]{8,17}", route_table_id):
                    msg = f"Invalid route table id: {route_table_id!r}"
                    raise ValueError(msg)
        if self.existing_endpoints and not self.uses_existing_vpc:
            msg = "existing_endpoints is only valid together with an existing VPC"
            raise ValueError(msg)
        for entry in self.existing_endpoints:
            service, _, detail = entry.partition("=")
            if service not in _ENDPOINT_SERVICES:
                expected = sorted(_ENDPOINT_SERVICES)
                msg = f"Unknown endpoint service {service!r}; expected one of {expected}"
                raise ValueError(msg)
            if service in _GATEWAY_SERVICES:
                if detail:
                    msg = f"Gateway endpoint {service!r} takes no endpoint/security-group detail"
                    raise ValueError(msg)
            else:
                endpoint_id, _, sg_id = detail.partition(":")
                if not re.fullmatch(r"vpce-[0-9a-f]{8,17}", endpoint_id) or not re.fullmatch(
                    r"sg-[0-9a-f]{8,17}", sg_id
                ):
                    msg = (
                        f"Interface endpoint {service!r} must be given as "
                        f"{service}=vpce-<id>:sg-<id> (got {entry!r})"
                    )
                    raise ValueError(msg)
        if self.aurora_min_acu > self.aurora_max_acu:
            msg = "aurora_min_acu must not exceed aurora_max_acu"
            raise ValueError(msg)
        if (self.aurora_min_acu * 2) % 1 or (self.aurora_max_acu * 2) % 1:
            msg = "Aurora capacity must be a multiple of 0.5 ACU"
            raise ValueError(msg)
        if self.work_hours_min_tasks is not None:
            for name, lane in (("hl7", self.hl7), ("ccda", self.ccda)):
                if not lane.min_tasks <= self.work_hours_min_tasks <= lane.max_tasks:
                    msg = f"work_hours_min_tasks must be between {name} min_tasks and max_tasks"
                    raise ValueError(msg)
        try:
            ZoneInfo(self.work_hours_timezone)
        except (ZoneInfoNotFoundError, ValueError):
            msg = f"Unknown work_hours_timezone {self.work_hours_timezone!r}"
            raise ValueError(msg) from None
        if self.account is not None and self.dashboard_principal_arn is not None:
            principal_account = self.dashboard_principal_arn.split(":", maxsplit=5)[4]
            if principal_account != self.account:
                msg = "Dashboard principal must belong to the deployment account"
                raise ValueError(msg)
        return self

    @classmethod
    def from_context(cls, node: Node) -> Self:
        """Load configuration without requiring credentials for local synthesis."""
        raw_environment = node.try_get_context("environment") or "dev"
        environment = DeploymentEnvironment(str(raw_environment))
        termination_context = node.try_get_context("termination_protection")
        termination_protection = _context_bool(
            termination_context,
            default=environment is DeploymentEnvironment.PROD,
        )

        # Environment fallback supports CDK CLI deployment while keeping local synth optional.
        context_account = node.try_get_context("account")
        context_region = node.try_get_context("region")
        if context_account is None and context_region is None:
            environment_account = os.getenv("CDK_DEFAULT_ACCOUNT")
            environment_region = os.getenv("CDK_DEFAULT_REGION")
            if environment_account is not None and environment_region is not None:
                account = environment_account
                region = environment_region
            else:
                account = None
                region = None
        else:
            account = context_account
            region = context_region

        return cls(
            project_name=str(node.try_get_context("project_name") or "manifest-medex-data-quality"),
            environment=environment,
            account=account,
            region=region,
            enable_cdk_nag=_context_bool(
                node.try_get_context("enable_cdk_nag"),
                default=True,
            ),
            enable_public_dashboard=_context_bool(
                node.try_get_context("enable_public_dashboard"),
                default=False,
            ),
            dashboard_principal_arn=(
                str(value).strip()
                if (value := node.try_get_context("dashboard_principal_arn")) is not None
                else None
            ),
            termination_protection=termination_protection,
            vpc_id=(str(value).strip() if (value := node.try_get_context("vpc_id")) else None),
            subnet_ids=_context_list(node.try_get_context("subnet_ids")),
            route_table_ids=_context_list(node.try_get_context("route_table_ids")),
            availability_zones=_context_list(node.try_get_context("availability_zones")),
            existing_endpoints=_context_list(node.try_get_context("existing_endpoints")),
            worker_image_digest=(
                str(value).strip()
                if (value := node.try_get_context("worker_image_digest"))
                else None
            ),
            hl7=_lane_from_context(node, "hl7", HL7_LANE_DEFAULTS),
            ccda=_lane_from_context(node, "ccda", CCDA_LANE_DEFAULTS),
            work_hours_min_tasks=(
                _context_int(value, default=0)
                if (value := node.try_get_context("work_hours_min_tasks")) not in (None, "")
                else None
            ),
            work_hours_timezone=str(
                node.try_get_context("work_hours_timezone") or "America/Los_Angeles"
            ),
            aurora_min_acu=_context_float(node.try_get_context("aurora_min_acu"), default=2.0),
            aurora_max_acu=_context_float(node.try_get_context("aurora_max_acu"), default=16.0),
        )
