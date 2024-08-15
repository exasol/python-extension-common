import os
import pytest
from unittest.mock import patch, create_autospec
import click

from contextlib import ExitStack
from click.testing import CliRunner
from exasol.python_extension_common.deployment.language_container_deployer_cli import (
    _ParameterFormatters,
    CustomizableParameters,
    language_container_deployer_main,
    SecretParams,
)
from exasol.python_extension_common.deployment.language_container_deployer import LanguageContainerDeployer


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
            cli: str = None,
            env: str = None,
            prompt: str = None,
    ):
        self.api_kwarg = api
        self.value = f"om_value_{api}" if value is None else value
        self.cli = cli or "--" + api.replace("_", "-")
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


def test_deployer_cli_with_missing_container_option():
    result = CliRunner().invoke(
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
        "Incomplete parameter list." in str(result.exception)


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
    ]
    deployer = create_autospec(LanguageContainerDeployer)
    with patch.object(LanguageContainerDeployer,
                      "create", return_value=deployer) as create:
        result = CliRunner().invoke(
            language_container_deployer_main,
            ["--container-file", container_file])

    actual = create.call_args.kwargs
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
        # For the following two arguments to
        # LanguageContainerDeployer.create() there are no corresponding CLI
        # options defined:
        # - container_url: Optional[str] = None,
        # - container_name: Optional[str] = None):
    ]
    def keys_and_values():
        for o in options:
            if o.value == False:
                yield "--no-" + o.cli[2:]
                continue
            yield o.cli
            yield str(o.value)

    cli_options = list(keys_and_values())
    with patch.object(LanguageContainerDeployer, "create", autospec=True) as create:
        result = CliRunner().invoke(
            language_container_deployer_main,
            ["--no-upload_container"] + cli_options )
    # print(f'exit code: {result.exit_code}, output: >{result.output}<')
    # print(f'exception: {result.exception}<')
    actual = create.call_args.kwargs
    for o in options:
        assert actual[o.api_kwarg] == o.value


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
    with patch.object(LanguageContainerDeployer, "create", autospec=True) as create:
        result = CliRunner().invoke(
            language_container_deployer_main,
            ["--no-upload_container", option.cli] )
    assert result.output.startswith(option.prompt)


def test_secrets_from_env():
    env_options = [
        OptionMapper.from_secret_param(p)
        for p in SecretParams
    ]
    patch_env = { o.env: o.value for o in env_options }
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, patch_env, clear=True))
        create = stack.enter_context(patch.object(LanguageContainerDeployer, "create", autospec=True))
        result = CliRunner().invoke(
            language_container_deployer_main,
            ["--no-upload_container"])

    actual = create.call_args.kwargs
    for o in env_options:
        assert actual[o.api_kwarg] == o.value


# Additionally there seems to be a file main.py missing that is wrapping the
# command line.
