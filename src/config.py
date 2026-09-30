"""Validate deployment context and enforce environment-specific safety constraints."""

import os
from enum import StrEnum
from typing import Self

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
        )
