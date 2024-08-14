import click
from click.testing import CliRunner
from exasol.python_extension_common.deployment.language_container_deployer_cli import (
    _ParameterFormatters,
    CustomizableParameters,
    language_container_deployer_main,
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
    # print(f'\nExitCode: {result.exit_code}')
    # print(f'\nOutput:>{result.output}<')
    # print(f'\nException: {type(result.exception)}: >{result.exception}<')
    assert result.exit_code == 1 and \
        isinstance(result.exception, ValueError) and \
        "Incomplete parameter list." in str(result.exception)
