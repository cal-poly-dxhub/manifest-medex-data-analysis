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
