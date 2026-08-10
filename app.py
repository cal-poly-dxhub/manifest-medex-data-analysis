from typing import cast

from aws_cdk import App, Aspects, Environment, IAspect
from cdk_nag import AwsSolutionsChecks
from src.config import AppConfig
from src.stack import DataQualityStack


def build_app() -> App:
    """Build the CDK application without performing any AWS API calls."""
    app = App()
    config = AppConfig.from_context(app.node)
    environment = (
        Environment(account=config.account, region=config.region)
        if config.account is not None and config.region is not None
        else None
    )

    DataQualityStack(
        app,
        f"{config.project_name}-{config.environment.value}",
        config=config,
        env=environment,
    )

    if config.enable_cdk_nag:
        Aspects.of(app).add(cast(IAspect, AwsSolutionsChecks(verbose=True)))

    return app


if __name__ == "__main__":
    build_app().synth()
