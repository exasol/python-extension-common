import shlex

import click
import pytest
from click.testing import CliRunner

from exasol.python_extension_common.cli.std_options import (
    SECRET_DISPLAY,
    ParameterFormatters,
    StdParams,
    StdTags,
    check_params,
    create_std_option,
    decode_secret_value,
    encode_secret_value,
    get_bool_opt_name,
    get_cli_arg,
    get_opt_name,
    is_secret_param,
    kwargs_to_cli_args,
    select_std_options,
)


def test_parameter_formatters_1param():
    container_url_param = "container_url"
    cmd = click.Command("a_command")
    ctx = click.Context(cmd)
    opt = click.Option(["--version"])
    formatters = ParameterFormatters()
    formatters.set_formatter(container_url_param, "http://my_server/{version}/my_stuff")
    formatters(ctx, opt, "1.3.2")
    assert ctx.params[container_url_param] == "http://my_server/1.3.2/my_stuff"


def test_parameter_formatters_2params():
    container_url_param = "container_url"
    container_name_param = "container_name"
    cmd = click.Command("a_command")
    ctx = click.Context(cmd)
    opt1 = click.Option(["--version"])
    opt2 = click.Option(["--user"])
    formatters = ParameterFormatters()
    formatters.set_formatter(container_url_param, "http://my_server/{version}/{user}/my_stuff")
    formatters.set_formatter(container_name_param, "downloaded-{version}")
    formatters(ctx, opt1, "1.3.2")
    formatters(ctx, opt2, "cezar")
    assert ctx.params[container_url_param] == "http://my_server/1.3.2/cezar/my_stuff"
    assert ctx.params[container_name_param] == "downloaded-1.3.2"


def test_get_opt_name():
    assert get_opt_name("db_user") == "--db-user"


def test_get_bool_opt_name():
    assert get_bool_opt_name("alter_system") == "--alter-system/--no-alter-system"


def test_create_std_option():
    opt = create_std_option(StdParams.bucketfs_name, type=str)
    assert opt.name == StdParams.bucketfs_name.name


def test_create_std_option_bool():
    opt = create_std_option(StdParams.allow_override, type=bool)
    assert opt.name == StdParams.allow_override.name
    assert "--no-allow-override" in opt.secondary_opts


def test_create_std_option_secret():
    opt = create_std_option(StdParams.db_password, type=str, hide_input=True)
    assert opt.hide_input
    assert not opt.prompt_required
    assert opt.default == SECRET_DISPLAY


def test_create_std_option_arbitrary_name():
    opt_name = "xyz"
    opt = create_std_option(opt_name, type=str)
    assert opt.name == opt_name


def test_select_std_options():
    for tag in StdTags:
        opts = {opt.name for opt in select_std_options(tag)}
        expected_opts = {std_param.name for std_param in StdParams if tag in std_param.tags}
        assert opts == expected_opts


def test_select_std_options_all():
    opts = {opt.name for opt in select_std_options("all")}
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
    expected_opts_bfs = {std_param.name for std_param in StdParams if StdTags.BFS in std_param.tags}
    expected_opts_slc = {std_param.name for std_param in StdParams if StdTags.SLC in std_param.tags}
    expected_opts = expected_opts_bfs.union(expected_opts_slc)
    assert opts == expected_opts


def test_select_std_options_with_exclude():
    opts = [opt.name for opt in select_std_options(StdTags.SLC, exclude=StdParams.language_alias)]
    assert StdParams.language_alias.name not in opts


def test_select_std_options_with_override():
    opts = {
        opt.name: opt
        for opt in select_std_options(
            StdTags.SLC, override={StdParams.alter_system: {"type": bool, "default": False}}
        )
    }
    assert not opts[StdParams.alter_system.name].default


def test_select_std_options_with_formatter():
    container_url_arg = "container_url"
    container_name_arg = "container_name"
    url_format = "https://my_service_url/{version}/page"
    name_format = "my_service_name"
    version = "4.5.6"
    expected_url = url_format.format(version=version)
    expected_name = name_format

    def func(**kwargs):
        assert kwargs[container_name_arg] == expected_name
        assert kwargs[container_url_arg] == expected_url

    ver_formatters = ParameterFormatters()
    ver_formatters.set_formatter(container_url_arg, url_format)
    ver_formatters.set_formatter(container_name_arg, name_format)

    opts = select_std_options(StdTags.SLC, formatters={StdParams.version: ver_formatters})
    cmd = click.Command("do_something", params=opts, callback=func)
    runner = CliRunner()
    runner.invoke(cmd, args=f"--version {version}", catch_exceptions=False, standalone_mode=False)


def test_hidden_opt_with_envar(monkeypatch):
    """
    This test checks the mechanism of providing a value of a confidential parameter
    via an environment variable.

    Regression test for #173: the env var name must be underscored (DB_PASSWORD), not
    the hyphenated form of the CLI flag (DB-PASSWORD), since the latter can't even be
    set via `export` in a real shell.
    """
    std_param = StdParams.db_password
    envar_name = "DB_PASSWORD"
    param_value = "my_password"

    captured = {}

    def func(**kwargs):
        captured.update(kwargs)

    opt = create_std_option(std_param, type=str, hide_input=True)
    cmd = click.Command("do_something", params=[opt], callback=func)
    runner = CliRunner()
    monkeypatch.setenv(envar_name, param_value)
    result = runner.invoke(cmd, catch_exceptions=False, standalone_mode=False)
    assert result.exit_code == 0
    assert captured[std_param.name] == param_value


_QUOTE_INSIDE_VALUE = 'quote"inside'


@pytest.mark.parametrize(
    ["std_param", "param_value", "expected_result"],
    [
        (StdParams.db_user, "Me", "--db-user Me"),
        ("user_rating", 5, "--user-rating 5"),
        (StdParams.use_ssl_cert_validation, True, "--use-ssl-cert-validation"),
        (StdParams.use_ssl_cert_validation, False, "--no-use-ssl-cert-validation"),
        (
            StdParams.db_user,
            _QUOTE_INSIDE_VALUE,
            f"--db-user {shlex.quote(_QUOTE_INSIDE_VALUE)}",
        ),
    ],
)
def test_get_cli_arg(std_param, param_value, expected_result):
    assert get_cli_arg(std_param, param_value) == expected_result


def test_kwargs_to_cli_args():
    arg_string = kwargs_to_cli_args(use_rgb=True, colour="Blue", compress_image=False)
    arg_set = set(arg_string.split())
    expected_set = {"--use-rgb", "--colour", "Blue", "--no-compress-image"}
    assert arg_set == expected_set


def test_get_cli_arg_value_with_double_quote_survives_shlex_round_trip():
    """
    Regression test: get_cli_arg used to wrap the value in unescaped literal double
    quotes, so a value containing '"' produced an args string that shlex/click can't
    parse (the same class of bug as #168, just triggered by a different character).
    """
    value = 'pa"ss'
    arg = get_cli_arg(StdParams.db_user, value)
    assert shlex.split(arg) == ["--db-user", value]


@pytest.mark.parametrize(
    ["std_param", "expected"],
    [
        (StdParams.saas_database_id, True),
        (StdParams.db_password, True),
        (StdParams.bucketfs_password, True),
        (StdParams.saas_account_id, True),
        (StdParams.saas_token, True),
        (StdParams.db_user, False),
        ("saas_database_id", True),
        ("db_user", False),
        ("not_a_std_param", False),
    ],
)
def test_is_secret_param(std_param, expected):
    assert is_secret_param(std_param) is expected


@pytest.mark.parametrize(
    "value",
    [
        "--dI0m90RUKefql382tsWA",
        "-dashy",
        "---triple-dash",
        "-",
        "",
        # Values that themselves contain the reserved escape-boundary character
        # (U+2010), which decode_secret_value used to always treat as its own
        # encoding, corrupting a value that legitimately starts with it.
        "‐2-",
        "‐‐realtoken",
        "-‐‐foo",
        "‐",
    ],
)
def test_encode_decode_secret_value_roundtrip(value):
    assert decode_secret_value(encode_secret_value(value)) == value


def test_encode_secret_value_leaves_unremarkable_values_unchanged():
    assert encode_secret_value("regular_value") == "regular_value"


@pytest.mark.parametrize(
    ["value", "expected_encoded"],
    [
        # Plain dash-prefixed values: the leading "-" run is replaced by the
        # escape-boundary character (U+2010, a lookalike click's parser doesn't
        # recognize as an option prefix) followed by its length, so the encoded
        # value no longer starts with an ASCII "-".
        ("--dI0m90RUKefql382tsWA", "‐2‐dI0m90RUKefql382tsWA"),
        ("-dashy", "‐1‐dashy"),
        ("---triple-dash", "‐3‐triple-dash"),
        ("-", "‐1‐"),
        # Values that themselves start with the escape-boundary character (but
        # not with an ASCII "-") still get the same two-part prefix, with a
        # leading-dash count of 0, so decode_secret_value can still tell them
        # apart from a "real" encoding of a dash-prefixed value.
        ("‐2-", "‐0‐‐2-"),
        ("‐‐realtoken", "‐0‐‐‐realtoken"),
        ("-‐‐foo", "‐1‐‐‐foo"),
        ("‐", "‐0‐‐"),
    ],
)
def test_encode_secret_value_escapes_leading_dashes(value, expected_encoded):
    """
    Regression test for the PR #174 review comment: test_encode_decode_secret_value_roundtrip
    only proves encode_secret_value and decode_secret_value are inverses of each other, not
    that encoding actually strips the leading "-"/_ESCAPE_BOUNDARY that click's parser
    chokes on. This pins down the exact encoded form instead.
    """
    encoded = encode_secret_value(value)
    assert encoded == expected_encoded
    assert not encoded.startswith("-")


def test_get_cli_arg_secret_param_with_dash_prefixed_value():
    """
    Regression test for #168. A secret option's value that itself starts with "-"
    breaks click's parser (see docstring of get_cli_arg for why), regardless of
    whether it's joined to the option with a space or "=". get_cli_arg works around
    this by encoding the leading dash(es) instead of putting them on the command
    line literally.
    """
    dashy_value = "--dI0m90RUKefql382tsWA"
    arg = get_cli_arg(StdParams.saas_database_id, dashy_value)
    assert arg == f"--saas-database-id {shlex.quote(encode_secret_value(dashy_value))}"


def test_get_cli_arg_secret_param_end_to_end_via_click():
    """
    End-to-end regression test for #168: builds the real saas_database_id option and
    invokes it through click, with a value that used to raise NoSuchOption.
    """
    dashy_value = "--dI0m90RUKefql382tsWA"
    opt = create_std_option(StdParams.saas_database_id, type=str, hide_input=True)

    captured = {}

    def func(**kwargs):
        captured.update(kwargs)

    cmd = click.Command("do_something", params=[opt], callback=func)
    arg_string = kwargs_to_cli_args(saas_database_id=dashy_value)

    runner = CliRunner()
    result = runner.invoke(cmd, args=arg_string, catch_exceptions=False, standalone_mode=False)

    assert result.exit_code == 0
    assert captured["saas_database_id"] == dashy_value


def test_get_cli_arg_secret_params_survive_full_saas_option_set():
    """
    Regression test for #168, exercised through the full option set built the same
    way LanguageContainerDeployerCli's SaaS CLI is (select_std_options over DB|SAAS,
    BFS|SAAS, SLC tags), to guard against a secret param being silently dropped when
    mixed in with many other options.
    """
    opts = select_std_options([StdTags.DB | StdTags.SAAS, StdTags.BFS | StdTags.SAAS, StdTags.SLC])
    captured = {}

    def func(**kwargs):
        captured.update(kwargs)

    cmd = click.Command("deploy_slc", params=opts, callback=func)

    saas_cli_args = {
        StdParams.saas_url.name: "https://cloud.exasol.com",
        StdParams.saas_account_id.name: "--saas-acct-dashy",
        StdParams.saas_database_id.name: "--dI0m90RUKefql382tsWA",
        StdParams.saas_token.name: "--saas-token-dashy",
        StdParams.path_in_bucket.name: "container",
        StdParams.language_alias.name: "PYTHON3_MY_LANG",
    }
    slc_cli_args = {
        StdParams.alter_system.name: True,
        StdParams.allow_override.name: True,
        StdParams.wait_for_completion.name: True,
    }
    extra_cli_args = {StdParams.version.name: "1.2.3"}

    arg_string = kwargs_to_cli_args(**saas_cli_args, **slc_cli_args, **extra_cli_args)
    runner = CliRunner()
    result = runner.invoke(cmd, args=arg_string, catch_exceptions=False, standalone_mode=False)

    assert result.exit_code == 0
    for name in (
        StdParams.saas_account_id.name,
        StdParams.saas_database_id.name,
        StdParams.saas_token.name,
    ):
        assert captured[name] == saas_cli_args[name]


@pytest.mark.parametrize(
    ["std_params", "param_kwargs", "expected_result"],
    [
        (
            [StdParams.dsn, StdParams.db_user],
            {StdParams.dsn.name: "my_dsn", StdParams.db_user.name: "my_user_name"},
            True,
        ),
        (
            [StdParams.dsn, StdParams.db_user],
            {StdParams.dsn.name: "my_dsn", StdParams.db_password.name: "my_password"},
            False,
        ),
        (
            [StdParams.dsn, StdParams.db_user],
            {StdParams.dsn.name: "my_dsn", StdParams.db_user.name: ""},
            False,
        ),
        (
            [[StdParams.dsn, StdParams.db_user], [StdParams.saas_url, StdParams.saas_account_id]],
            {StdParams.dsn.name: "my_dsn", StdParams.db_user.name: "my_user_name"},
            False,
        ),
        (
            [[StdParams.dsn, StdParams.saas_url], [StdParams.db_user, StdParams.saas_account_id]],
            {StdParams.dsn.name: "my_dsn", StdParams.db_user.name: "my_user_name"},
            True,
        ),
        (
            [StdParams.dsn, StdParams.use_ssl_cert_validation],
            {StdParams.dsn.name: "my_dsn", StdParams.use_ssl_cert_validation.name: False},
            True,
        ),
        (
            StdParams.dsn,
            {StdParams.dsn.name: "my_dsn", StdParams.use_ssl_cert_validation.name: False},
            True,
        ),
        (
            [
                [StdParams.dsn.name, StdParams.db_user.name],
                [StdParams.saas_url.name, StdParams.saas_account_id.name],
            ],
            {StdParams.dsn.name: "my_dsn", StdParams.db_user.name: "my_user_name"},
            False,
        ),
        (
            [
                [StdParams.dsn.name, StdParams.saas_url.name],
                [StdParams.db_user.name, StdParams.saas_account_id.name],
            ],
            {StdParams.dsn.name: "my_dsn", StdParams.db_user.name: "my_user_name"},
            True,
        ),
    ],
)
def test_check_params(std_params, param_kwargs, expected_result):
    assert check_params(std_params, param_kwargs) == expected_result
