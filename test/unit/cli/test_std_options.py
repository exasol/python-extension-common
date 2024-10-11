import os
import click
from click.testing import CliRunner
import pytest

from exasol.python_extension_common.cli.std_options import (
    ParameterFormatters,
    SECRET_DISPLAY,
    StdTags,
    StdParams,
    create_std_option,
    select_std_options,
    get_cli_arg,
    check_params
)


def test_parameter_formatters_1param():
    container_url_param = 'container_url'
    cmd = click.Command('a_command')
    ctx = click.Context(cmd)
    opt = click.Option(['--version'])
    formatters = ParameterFormatters()
    formatters.set_formatter(container_url_param, 'http://my_server/{version}/my_stuff')
    formatters(ctx, opt, '1.3.2')
    assert ctx.params[container_url_param] == 'http://my_server/1.3.2/my_stuff'


def test_parameter_formatters_2params():
    container_url_param = 'container_url'
    container_name_param = 'container_name'
    cmd = click.Command('a_command')
    ctx = click.Context(cmd)
    opt1 = click.Option(['--version'])
    opt2 = click.Option(['--user'])
    formatters = ParameterFormatters()
    formatters.set_formatter(container_url_param, 'http://my_server/{version}/{user}/my_stuff')
    formatters.set_formatter(container_name_param, 'downloaded-{version}')
    formatters(ctx, opt1, '1.3.2')
    formatters(ctx, opt2, 'cezar')
    assert ctx.params[container_url_param] == 'http://my_server/1.3.2/cezar/my_stuff'
    assert ctx.params[container_name_param] == 'downloaded-1.3.2'


def test_create_std_option():
    opt = create_std_option(StdParams.bucketfs_name, type=str)
    assert opt.name == StdParams.bucketfs_name.name


def test_create_std_option_bool():
    opt = create_std_option(StdParams.allow_override, type=bool)
    assert opt.name == StdParams.allow_override.name
    assert '--no-allow-override' in opt.secondary_opts


def test_create_std_option_secret():
    opt = create_std_option(StdParams.db_password, type=str, hide_input=True)
    assert opt.hide_input
    assert not opt.prompt_required
    assert opt.default == SECRET_DISPLAY


def test_create_std_option_arbitrary_name():
    opt_name = 'xyz'
    opt = create_std_option(opt_name, type=str)
    assert opt.name == opt_name


def test_select_std_options():
    for tag in StdTags:
        opts = {opt.name for opt in select_std_options(tag)}
        expected_opts = {std_param.name for std_param in StdParams if tag in std_param.tags}
        assert opts == expected_opts


def test_select_std_options_all():
    opts = {opt.name for opt in select_std_options('all')}
    expected_opts = {std_param.name for std_param in StdParams}
    assert opts == expected_opts


def test_select_std_options_restricted():
    opts = {opt.name for opt in select_std_options(StdTags.BFS)}
    opts_onprem = {opt.name for opt in select_std_options(StdTags.BFS | StdTags.ONPREM)}
    assert opts_onprem
    assert len(opts) > len(opts_onprem)
    assert opts.intersection(opts_onprem) == opts_onprem


def test_select_std_options_multi_tags():
    opts = {opt.name for opt in select_std_options([StdTags.BFS, StdTags.SLC])}
    expected_opts_bfs = {std_param.name for std_param in StdParams
                         if StdTags.BFS in std_param.tags}
    expected_opts_slc = {std_param.name for std_param in StdParams
                         if StdTags.SLC in std_param.tags}
    expected_opts = expected_opts_bfs.union(expected_opts_slc)
    assert opts == expected_opts


def test_select_std_options_with_exclude():
    opts = [opt.name for opt in select_std_options(StdTags.SLC,
                                                   exclude=StdParams.language_alias)]
    assert StdParams.language_alias.name not in opts


def test_select_std_options_with_override():
    opts = {opt.name: opt for opt in select_std_options(
        StdTags.SLC, override={StdParams.alter_system: {'type': bool, 'default': False}})}
    assert not opts[StdParams.alter_system.name].default


def test_select_std_options_with_formatter():
    container_url_arg = 'container_url'
    container_name_arg = 'container_name'
    url_format = "https://my_service_url/{version}/page"
    name_format = "my_service_name"
    version = '4.5.6'
    expected_url = url_format.format(version=version)
    expected_name = name_format

    def func(**kwargs):
        assert kwargs[container_name_arg] == expected_name
        assert kwargs[container_url_arg] == expected_url

    ver_formatter = ParameterFormatters()
    ver_formatter.set_formatter(container_url_arg, url_format)
    ver_formatter.set_formatter(container_name_arg, name_format)

    opts = select_std_options(StdTags.SLC, formatters={StdParams.version: ver_formatter})
    cmd = click.Command('do_something', params=opts, callback=func)
    runner = CliRunner()
    runner.invoke(cmd, args=f'--version {version}', catch_exceptions=False, standalone_mode=False)


def test_hidden_opt_with_envar():
    """
    This test checks the mechanism of providing a value of a confidential parameter
    via an environment variable.
    """
    std_param = StdParams.db_password
    envar_name = std_param.name.upper()
    param_value = 'my_password'

    def func(**kwargs):
        assert std_param.name in kwargs
        assert kwargs[std_param.name] == param_value

    opt = create_std_option(std_param, type=str, hide_input=True)
    cmd = click.Command('do_something', params=[opt], callback=func)
    runner = CliRunner()
    os.environ[envar_name] = param_value
    try:
        runner.invoke(cmd)
    finally:
        os.environ.pop(envar_name)


@pytest.mark.parametrize(
    ['std_param', 'param_value', 'expected_result'],
    [
        (StdParams.db_user, 'Me', '--db-user "Me"'),
        ('user_rating', 5, '--user-rating "5"'),
        (StdParams.use_ssl_cert_validation, True, '--use-ssl-cert-validation'),
        (StdParams.use_ssl_cert_validation, False, '--no-use-ssl-cert-validation')
    ]
)
def test_get_cli_arg(std_param, param_value, expected_result):
    assert get_cli_arg(std_param, param_value) == expected_result


@pytest.mark.parametrize(
    ['std_params', 'param_kwargs', 'expected_result'],
    [
        (
            [StdParams.dsn, StdParams.db_user],
            {StdParams.dsn.name: 'my_dsn', StdParams.db_user.name: 'my_user_name'},
            True
        ),
        (
            [StdParams.dsn, StdParams.db_user],
            {StdParams.dsn.name: 'my_dsn', StdParams.db_password.name: 'my_password'},
            False
        ),
        (
            [StdParams.dsn, StdParams.db_user],
            {StdParams.dsn.name: 'my_dsn', StdParams.db_user.name: ''},
            False
        ),
        (
            [[StdParams.dsn, StdParams.db_user], [StdParams.saas_url, StdParams.saas_account_id]],
            {StdParams.dsn.name: 'my_dsn', StdParams.db_user.name: 'my_user_name'},
            False,
        ),
        (
            [[StdParams.dsn, StdParams.saas_url], [StdParams.db_user, StdParams.saas_account_id]],
            {StdParams.dsn.name: 'my_dsn', StdParams.db_user.name: 'my_user_name'},
            True,
        ),
        (
            [StdParams.dsn, StdParams.use_ssl_cert_validation],
            {StdParams.dsn.name: 'my_dsn', StdParams.use_ssl_cert_validation.name: False},
            True
        ),
        (
            StdParams.dsn,
            {StdParams.dsn.name: 'my_dsn', StdParams.use_ssl_cert_validation.name: False},
            True
        ),
        (
                [[StdParams.dsn.name, StdParams.db_user.name],
                 [StdParams.saas_url.name, StdParams.saas_account_id.name]],
                {StdParams.dsn.name: 'my_dsn', StdParams.db_user.name: 'my_user_name'},
                False,
        ),
        (
                [[StdParams.dsn.name, StdParams.saas_url.name],
                 [StdParams.db_user.name, StdParams.saas_account_id.name]],
                {StdParams.dsn.name: 'my_dsn', StdParams.db_user.name: 'my_user_name'},
                True,
        ),
    ]
)
def test_check_params(std_params, param_kwargs, expected_result):
    assert check_params(std_params, param_kwargs) == expected_result
