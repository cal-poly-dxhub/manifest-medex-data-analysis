import pytest
from aws_cdk import App
from pydantic import ValidationError
from src.config import AppConfig, DeploymentEnvironment


def test_default_context_is_environment_agnostic_development() -> None:
    config = AppConfig.from_context(App().node)

    assert config.environment is DeploymentEnvironment.DEV
    assert config.account is None
    assert config.region is None
    assert config.enable_cdk_nag is True
    assert config.enable_public_dashboard is False
    assert config.dashboard_principal_arn is None
    assert config.termination_protection is False


def test_string_boolean_context_is_parsed_safely() -> None:
    app = App(
        context={
            "environment": "dev",
            "enable_cdk_nag": "false",
            "termination_protection": "true",
        }
    )

    config = AppConfig.from_context(app.node)
    assert config.enable_cdk_nag is False
    assert config.termination_protection is True


def test_production_defaults_to_termination_protection() -> None:
    config = AppConfig.from_context(App(context={"environment": "prod"}).node)

    assert config.environment is DeploymentEnvironment.PROD
    assert config.termination_protection is True


def test_production_cannot_disable_termination_protection() -> None:
    app = App(
        context={
            "environment": "prod",
            "termination_protection": "false",
        }
    )

    with pytest.raises(ValidationError, match="requires termination protection"):
        AppConfig.from_context(app.node)


def test_account_and_region_are_required_together() -> None:
    with pytest.raises(ValidationError, match="must be provided together"):
        AppConfig(
            project_name="manifest-medex-data-quality",
            environment=DeploymentEnvironment.DEV,
            account="111122223333",
        )


def test_invalid_boolean_context_is_rejected() -> None:
    app = App(context={"enable_cdk_nag": "sometimes"})

    with pytest.raises(ValueError, match="Invalid boolean context value"):
        AppConfig.from_context(app.node)


def test_development_public_dashboard_requires_matching_role_arn() -> None:
    role_arn = "arn:aws:iam::111122223333:role/DevelopmentDashboardRole"
    app = App(
        context={
            "environment": "dev",
            "account": "111122223333",
            "region": "us-west-2",
            "enable_public_dashboard": "true",
            "dashboard_principal_arn": role_arn,
        }
    )

    config = AppConfig.from_context(app.node)

    assert config.enable_public_dashboard is True
    assert config.dashboard_principal_arn == role_arn


@pytest.mark.parametrize("environment", ["staging", "prod"])
def test_public_dashboard_is_rejected_outside_development(environment: str) -> None:
    app = App(
        context={
            "environment": environment,
            "enable_public_dashboard": "true",
            "dashboard_principal_arn": "arn:aws:iam::111122223333:role/DashboardRole",
        }
    )

    with pytest.raises(ValidationError, match="allowed only in development"):
        AppConfig.from_context(app.node)


def test_public_dashboard_requires_principal() -> None:
    app = App(context={"enable_public_dashboard": "true"})

    with pytest.raises(ValidationError, match="requires dashboard_principal_arn"):
        AppConfig.from_context(app.node)


def test_dashboard_principal_is_rejected_when_feature_is_disabled() -> None:
    app = App(context={"dashboard_principal_arn": "arn:aws:iam::111122223333:role/DashboardRole"})

    with pytest.raises(ValidationError, match="requires enable_public_dashboard=true"):
        AppConfig.from_context(app.node)


def test_dashboard_principal_must_match_deployment_account() -> None:
    app = App(
        context={
            "account": "111122223333",
            "region": "us-west-2",
            "enable_public_dashboard": "true",
            "dashboard_principal_arn": "arn:aws:iam::444455556666:role/DashboardRole",
        }
    )

    with pytest.raises(ValidationError, match="must belong to the deployment account"):
        AppConfig.from_context(app.node)


def test_dashboard_principal_must_be_an_iam_role_arn() -> None:
    app = App(
        context={
            "enable_public_dashboard": "true",
            "dashboard_principal_arn": "arn:aws:sts::111122223333:assumed-role/Role/session",
        }
    )

    with pytest.raises(ValidationError, match="dashboard_principal_arn"):
        AppConfig.from_context(app.node)


_VPC_ID = "vpc-0123456789abcdef0"
_SUBNETS = ("subnet-0123456789abcdef0", "subnet-0123456789abcdef1")
_ROUTE_TABLES = ("rtb-0123456789abcdef0", "rtb-0123456789abcdef1")
_AZS = ("us-west-2a", "us-west-2b")


def test_existing_vpc_requires_all_four_settings_together() -> None:
    with pytest.raises(ValueError, match="together"):
        AppConfig(
            project_name="manifest-medex-data-quality",
            environment=DeploymentEnvironment.DEV,
            vpc_id=_VPC_ID,
        )


def test_existing_vpc_requires_two_availability_zones() -> None:
    with pytest.raises(ValueError, match="two Availability Zones"):
        AppConfig(
            project_name="manifest-medex-data-quality",
            environment=DeploymentEnvironment.DEV,
            vpc_id=_VPC_ID,
            subnet_ids=_SUBNETS,
            route_table_ids=_ROUTE_TABLES,
            availability_zones=("us-west-2a", "us-west-2a"),
        )


def test_existing_vpc_requires_one_route_table_per_subnet() -> None:
    with pytest.raises(ValueError, match="one route table per subnet"):
        AppConfig(
            project_name="manifest-medex-data-quality",
            environment=DeploymentEnvironment.DEV,
            vpc_id=_VPC_ID,
            subnet_ids=_SUBNETS,
            route_table_ids=("rtb-0123456789abcdef0",),
            availability_zones=_AZS,
        )


def test_existing_vpc_accepts_complete_settings() -> None:
    config = AppConfig(
        project_name="manifest-medex-data-quality",
        environment=DeploymentEnvironment.DEV,
        vpc_id=_VPC_ID,
        subnet_ids=_SUBNETS,
        route_table_ids=_ROUTE_TABLES,
        availability_zones=_AZS,
    )
    assert config.uses_existing_vpc is True


def test_default_config_creates_its_own_vpc() -> None:
    config = AppConfig(
        project_name="manifest-medex-data-quality", environment=DeploymentEnvironment.DEV
    )
    assert config.uses_existing_vpc is False


def test_existing_endpoints_require_existing_vpc() -> None:
    with pytest.raises(ValueError, match="only valid together with an existing VPC"):
        AppConfig(
            project_name="manifest-medex-data-quality",
            environment=DeploymentEnvironment.DEV,
            existing_endpoints=("s3",),
        )


def test_existing_interface_endpoint_needs_endpoint_and_security_group_ids() -> None:
    with pytest.raises(ValueError, match="sqs=vpce-<id>:sg-<id>"):
        AppConfig(
            project_name="manifest-medex-data-quality",
            environment=DeploymentEnvironment.DEV,
            vpc_id=_VPC_ID,
            subnet_ids=_SUBNETS,
            route_table_ids=_ROUTE_TABLES,
            availability_zones=_AZS,
            existing_endpoints=("sqs",),
        )


def test_existing_endpoint_map_parses_gateway_and_interface_entries() -> None:
    config = AppConfig(
        project_name="manifest-medex-data-quality",
        environment=DeploymentEnvironment.DEV,
        vpc_id=_VPC_ID,
        subnet_ids=_SUBNETS,
        route_table_ids=_ROUTE_TABLES,
        availability_zones=_AZS,
        existing_endpoints=("s3", "lambda=vpce-0123456789abcdef0:sg-0123456789abcdef0"),
    )
    assert config.existing_endpoint_map == {
        "s3": None,
        "lambda": ("vpce-0123456789abcdef0", "sg-0123456789abcdef0"),
    }
