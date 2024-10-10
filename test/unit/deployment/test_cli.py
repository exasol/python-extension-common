import os
import pytest
from unittest.mock import patch, create_autospec, Mock
import click
import click.testing

from typing import Dict, List
from contextlib import ExitStack, contextmanager
from exasol.python_extension_common.deployment.language_container_deployer_cli import (
    _ParameterFormatters,
    CustomizableParameters,
    language_container_deployer_main,
    SecretParams,
)
from exasol.python_extension_common.deployment.language_container_deployer import LanguageContainerDeployer
from datetime import timedelta


class CliRunner:
    def __init__(self, deployer: Mock = Mock()):
        self.deployer = deployer
        self.context = deployer
        self.create = None
        self.result = None

    def __enter__(self):
        self.context = patch.object(
            LanguageContainerDeployer,
            "create",
            return_value=self.deployer,
            autospec=True,
        )
        self.create = self.context.__enter__()
        return self

    def run(self, *args: List[str]):
        self.result = click.testing.CliRunner().invoke(
            language_container_deployer_main,
            list(args),
        )
        return self.result

    def __exit__(self, type, value, traceback):
        self.context.__exit__(type, value, traceback)


@pytest.fixture
def container_file(tmp_path):
    file = tmp_path / "container.tgz"
    file.touch()
    return file


class OptionMapper:
    """
    This class enables to align the corresponding names of
    * CLI option
    * kwarg in call to LanguageContainerDeployer()
    * sample value to be verified in a test case
    * environment variable (optional)
    """
    def __init__(
            self,
            api: str,
            value: any = None,
            cli_value: any = None,
            cli: str = None,
            env: str = None,
            prompt: str = None,
    ):
        self.api_kwarg = api
        self.value = f"om_value_{api}" if value is None else value
        self.cli = cli or "--" + api.replace("_", "-")
        self.cli_value = self.value if cli_value is None else cli_value
        self.env = env
        self.prompt = prompt

    @classmethod
    def from_secret_param(
            cls,
            param: SecretParams,
            prompt: str = None,
    ) -> "OptionMapper":
        name = param.name
        return cls(
            api=name.lower(),
            cli=f"--{param.value}",
            env=name,
            prompt=prompt,
        )

def test_parameter_formatters_1param():
    cmd = click.Command('a_command')
    ctx = click.Context(cmd)
    opt = click.Option(['--version'])
    formatters = _ParameterFormatters()
    formatters.set_formatter(CustomizableParameters.container_url, 'http://my_server/{version}/my_stuff')
    formatters.set_formatter(CustomizableParameters.container_name, 'downloaded')
    formatters(ctx, opt, '1.3.2')
    assert ctx.params[CustomizableParameters.container_url.name] == 'http://my_server/1.3.2/my_stuff'
    assert ctx.params[CustomizableParameters.container_name.name] == 'downloaded'


def test_parameter_formatters_2params():
    cmd = click.Command('a_command')
    ctx = click.Context(cmd)
    opt1 = click.Option(['--version'])
    opt2 = click.Option(['--user'])
    formatters = _ParameterFormatters()
    formatters.set_formatter(CustomizableParameters.container_url, 'http://my_server/{version}/{user}/my_stuff')
    formatters.set_formatter(CustomizableParameters.container_name, 'downloaded-{version}')
    formatters(ctx, opt1, '1.3.2')
    formatters(ctx, opt2, 'cezar')
    assert ctx.params[CustomizableParameters.container_url.name] == 'http://my_server/1.3.2/cezar/my_stuff'
    assert ctx.params[CustomizableParameters.container_name.name] == 'downloaded-1.3.2'

import re
def test_deployer_cli_with_missing_container_option():
    result = click.testing.CliRunner().invoke(
        language_container_deployer_main,
        ["--language-alias", "PYTHON3_PEC_TESTS_CLI",
         "--bucketfs-user", "bfs-username",
         "--bucketfs-password", "bfs-password",
         "--dsn", "host:port",
         "--db-user", "db-username",
         "--db-pass", "db-password",
         ])
    assert result.exit_code == 1 and \
        isinstance(result.exception, ValueError) and \
        re.match((
            "Incomplete parameter list."
            ".*Please either provide the parameters"
            ".*for an On-Prem database or"
            ".*for a SaaS database."
        ), str(result.exception))


def test_default_values(container_file):
    """
    Verify that default values specified for CLI options are passed
    correctly to methods create() and run() of LanguageContainerDeployer.
    """
    create_options = [
        OptionMapper("saas_url", "https://cloud.exasol.com"),
        OptionMapper("language_alias", "PYTHON3_EXT"),
        OptionMapper("ssl_trusted_ca", "", cli="--ssl-cert-path"),
        OptionMapper("ssl_client_certificate", "", cli="--ssl-client-cert-path"),
        OptionMapper("ssl_private_key", "", cli="--ssl-client-private-key"),
        OptionMapper("use_ssl_cert_validation", True),
        OptionMapper("extract_timeout", cli="--extract-timeout-minutes",
                     cli_value=5, value=timedelta(minutes=10)),
        OptionMapper("display_progress", True),
    ]
    deployer = create_autospec(LanguageContainerDeployer)
    with CliRunner(deployer) as runner:
        runner.run("--container-file", container_file)

    actual = runner.create.call_args.kwargs
    for o in create_options:
        assert actual[o.api_kwarg] == o.value

    run_options = [
        OptionMapper("alter_system", True),
        OptionMapper("allow_override", False),
        OptionMapper("wait_for_completion", True),
    ]
    actual = deployer.run.call_args.kwargs
    for o in run_options:
        assert actual[o.api_kwarg] == o.value


def test_cli_options_passed_to_create(container_file):
    options = [
        OptionMapper("bucketfs_name"),
        OptionMapper("bucketfs_host"),
        OptionMapper("bucketfs_port", 123),
        OptionMapper("bucketfs_use_https", True),
        OptionMapper("bucketfs_user"),
        OptionMapper.from_secret_param(SecretParams.BUCKETFS_PASSWORD),
        OptionMapper("bucket"),
        OptionMapper("saas_url"),
        OptionMapper.from_secret_param(SecretParams.SAAS_ACCOUNT_ID),
        OptionMapper.from_secret_param(SecretParams.SAAS_DATABASE_ID),
        OptionMapper("saas_database_name"),
        OptionMapper.from_secret_param(SecretParams.SAAS_TOKEN),
        OptionMapper("path_in_bucket"),
        # OptionMapper("version"), option is unused
        OptionMapper("dsn"),
        OptionMapper("db_user"),
        OptionMapper.from_secret_param(SecretParams.DB_PASSWORD),
        OptionMapper("language_alias"),
        OptionMapper("ssl_trusted_ca", cli="--ssl-cert-path"),
        OptionMapper("ssl_client_certificate", cli="--ssl-client-cert-path"),
        OptionMapper("ssl_private_key", cli="--ssl-client-private-key"),
        OptionMapper("use_ssl_cert_validation", False),
        OptionMapper("extract_timeout", cli="--extract-timeout-minutes",
                     cli_value=6, value=timedelta(minutes=6)),
        OptionMapper("display_progress", False),
    ]
    def keys_and_values():
        for o in options:
            if o.value == False:
                yield "--no-" + o.cli[2:]
                continue
            yield o.cli
            yield str(o.cli_value)

    cli_options = list(keys_and_values())
    deployer = create_autospec(LanguageContainerDeployer)
    with CliRunner(deployer) as runner:
        runner.run("--no-upload_container", *cli_options)
    actual = runner.create.call_args.kwargs
    for o in options:
        assert actual[o.api_kwarg] == o.value
    assert deployer.run.called


@pytest.mark.parametrize(
    "param, prompt", [
        (SecretParams.DB_PASSWORD, "DB password"),
        (SecretParams.BUCKETFS_PASSWORD, "BucketFS password"),
        (SecretParams.SAAS_ACCOUNT_ID, "SaaS account id"),
        (SecretParams.SAAS_DATABASE_ID, "SaaS database id"),
        (SecretParams.SAAS_TOKEN, "SaaS token"),
        ])
def test_secret_options_prompt(param, prompt):
    option = OptionMapper.from_secret_param(param, prompt=prompt)
    with CliRunner() as runner:
        runner.run("--no-upload_container", option.cli)
    assert runner.result.output.startswith(option.prompt)


def test_secrets_from_env():
    env_options = [
        OptionMapper.from_secret_param(p)
        for p in SecretParams
    ]
    patched_env = { o.env: o.value for o in env_options }
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, patched_env, clear=True))
        runner = stack.enter_context(CliRunner())
        runner.run("--no-upload_container")

    actual = runner.create.call_args.kwargs
    for o in env_options:
        assert actual[o.api_kwarg] == o.value


def test_no_upload_container():
    "Covered by test_cli_options_passed_to_create()"


def test_container_file():
    "Covered by test_default_values()"


@pytest.mark.skip(reason="Test case to be implemented in derived applications")
def test_container_url():
    """
    For the following two arguments to LanguageContainerDeployer.create()
    the corresponding CLI options need to defined in actual CLI applications
    using ``language_container_deployer_main()``

    - container_url: Optional[str] = None,
    - container_name: Optional[str] = None):

    Hence there is no related test case in the current file.
    """
