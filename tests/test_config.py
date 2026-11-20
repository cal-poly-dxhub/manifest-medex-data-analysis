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


def test_ingestion_and_aurora_defaults_are_production_sized_in_every_environment() -> None:
    for environment in ("dev", "prod"):
        config = AppConfig.from_context(App(context={"environment": environment}).node)
        assert (config.hl7.task_cpu, config.hl7.task_memory) == (1024, 2048)
        assert (config.hl7.workers, config.hl7.receive_batch_size) == (8, 10)
        assert (config.hl7.min_tasks, config.hl7.max_tasks) == (2, 10)
        assert config.hl7.backlog_target == 1000
        assert (config.ccda.task_cpu, config.ccda.task_memory) == (1024, 8192)
        assert (config.hl7.max_object_mb, config.ccda.max_object_mb) == (512, 110)
        assert (config.ccda.workers, config.ccda.receive_batch_size) == (4, 1)
        assert (config.ccda.min_tasks, config.ccda.max_tasks) == (2, 10)
        assert config.ccda.backlog_target == 200
        assert (config.aurora_min_acu, config.aurora_max_acu) == (2.0, 16.0)
        assert config.work_hours_min_tasks is None
        assert config.worker_image_digest is None


def test_lane_and_aurora_overrides_are_read_from_context() -> None:
    digest = "sha256:" + "b" * 64
    app = App(
        context={
            "hl7_workers": "12",
            "hl7_max_tasks": "20",
            "ccda_task_memory": "8192",
            "ccda_backlog_target": "100",
            "work_hours_min_tasks": "4",
            "work_hours_timezone": "America/New_York",
            "aurora_min_acu": "1.5",
            "aurora_max_acu": "32",
            "worker_image_digest": digest,
        }
    )
    config = AppConfig.from_context(app.node)
    assert config.hl7.workers == 12
    assert config.hl7.max_tasks == 20
    assert config.hl7.task_memory == 2048
    assert config.ccda.task_memory == 8192
    assert config.ccda.backlog_target == 100
    assert config.work_hours_min_tasks == 4
    assert config.work_hours_timezone == "America/New_York"
    assert (config.aurora_min_acu, config.aurora_max_acu) == (1.5, 32.0)
    assert config.worker_image_digest == digest


@pytest.mark.parametrize(
    ("context", "message"),
    [
        ({"hl7_task_cpu": "512"}, "task_cpu must be one of"),
        ({"ccda_task_memory": "1024"}, "is not valid for task_cpu"),
        ({"hl7_min_tasks": "5", "hl7_max_tasks": "3"}, "min_tasks must not exceed max_tasks"),
        ({"hl7_receive_batch_size": "11"}, "less than or equal to 10"),
        ({"ccda_workers": "0"}, "greater than or equal to 1"),
        ({"hl7_workers": "many"}, "Invalid integer context value"),
        ({"aurora_min_acu": "8", "aurora_max_acu": "4"}, "must not exceed aurora_max_acu"),
        ({"aurora_max_acu": "16.3"}, "multiple of 0.5 ACU"),
        ({"work_hours_min_tasks": "11"}, "work_hours_min_tasks must be between"),
        ({"work_hours_timezone": "Mars/Olympus"}, "Unknown work_hours_timezone"),
        ({"worker_image_digest": "latest"}, "String should match pattern"),
    ],
)
def test_invalid_ingestion_settings_are_rejected(context: dict[str, str], message: str) -> None:
    # Malformed numbers fail while reading context (ValueError); out-of-range values fail model
    # validation (pydantic's ValidationError, itself a ValueError).
    with pytest.raises(ValueError, match=message):
        AppConfig.from_context(App(context=context).node)
