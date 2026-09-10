import os
import re
import shlex
from enum import (
    Enum,
    Flag,
    auto,
)
from typing import (
    Any,
    no_type_check,
)

import click

from exasol.python_extension_common.cli._param import Param


class ParameterFormatters:
    """
    The idea is that some of the CLI parameters can be programmatically
    customized based on values of other parameters and externally supplied
    patterns, called "formatters".

    Example: A specialized variant of the CLI may want to provide a custom URL
    "http://prefix/{version}/suffix" depending on CLI parameter "version".  If
    the user specifies version "1.2.3", then the default value for the URL
    should be updated to "http://prefix/1.2.3/suffix".

    The URL parameter in this example is called a _destination_ CLI parameter
    while the version is called _source_.

    A destination parameter can depend on a single or multiple source
    parameters.  In the previous example the URL could, for instance, also
    include a username: "http://prefix/{version}/{user}/suffix".

    The Click API allows updating customized parameters only in a callback
    function.  There is no way to inject them directly into the CLI, see the
    docs, as ``click.Option`` inherits from ``click.Parameter``:
    https://click.palletsprojects.com/en/stable/api/#click.Parameter.

    The current implementation updates the destination parameter only if the
    value of the source parameter is not ``None``.
    """

    def __init__(self) -> None:
        # Each key/value pair represents the name of a destination parameter
        # to update, and its default value.
        #
        # The default value can contain placeholders to be replaced by the
        # values of the other parameters, called "source parameters".
        self._formatters: dict[str, str] = {}

    def __call__(
        self, ctx: click.Context, source_param: click.Parameter, value: Any | None
    ) -> Any | None:
        def update(source: Param, dest: Param) -> None:
            if not dest.value:
                return None
            # Enclose in double curly brackets all other parameters in
            # dest.value to avoid error "missing parameters".
            #
            # Below is an example of a formatter string before and after
            # applying the regex, assuming the source parameter is 'version'.
            #
            # "something-with-{version}/tailored-for-{user}" =>
            # "something-with-{version}/tailored-for-{{user}}"
            #
            # We were looking for all occurrences of a pattern "{xxx}", where
            # xxx is not "version".
            pattern = r"\{(?!" + (source.name or "") + r"\})\w+\}"
            template = re.sub(pattern, lambda m: f"{{{m.group(0)}}}", dest.value)
            kwargs = {source.name: source.value}
            ctx.params[dest.name] = template.format(**kwargs)  # type: ignore

        source = Param(source_param.name, value)
        if source.value is not None:
            for name, default in self._formatters.items():
                value = ctx.params.get(name, default)
                update(source, dest=Param(name, value))

        return source.value

    def set_formatter(self, param_name: str, default_value: str) -> None:
        """
        Adds the specified destination parameter to be updated.

        Better arg names could be `destination_parameter` and `format_pattern`
        but renaming the arguments would break the public interface.

        Parameters:

          param_name: Name of the destination parameter to be updated.

          default_value: Pattern for the value to assign to the destination
                parameter. The pattern may contain place holders to be
                replaced by the values of other CLI parameters, called "source
                parameters".
        """
        self._formatters[param_name] = default_value

    def clear_formatters(self):
        """Deletes all destination parameters to be updated, mainly for testing purposes."""
        self._formatters.clear()


# This text will be displayed instead of the actual value for a "secret" option.
SECRET_DISPLAY = "***"

# A lookalike character used as a reserved delimiter in encode_secret_value's output
# (see there). Click's parser only recognizes the ASCII hyphen-minus (U+002D) as an
# option prefix, so this is invisible to it.
_ESCAPE_BOUNDARY = "‐"


def encode_secret_value(value: str) -> str:
    """
    Encodes value so that a secret option's value can be put on the command line
    without click's parser mistaking it for a new option (see get_cli_arg's
    docstring). Use decode_secret_value to reverse this.

    A value that neither starts with "-" (which click's parser would choke on) nor
    with _ESCAPE_BOUNDARY (which decode_secret_value would otherwise mistake for its
    own encoding) is returned unchanged. Otherwise, the value is encoded as
    _ESCAPE_BOUNDARY + <n> + _ESCAPE_BOUNDARY + <value with its leading "-" run of
    length n removed>, e.g. "--secret" -> "‐2‐secret". This length-prefixed form is
    an exact inverse for every possible input, including a value that itself starts
    with "-" and/or _ESCAPE_BOUNDARY, since decoding only ever needs the first two
    occurrences of _ESCAPE_BOUNDARY to recover n and the remainder verbatim.
    """
    if not value.startswith("-") and not value.startswith(_ESCAPE_BOUNDARY):
        return value
    stripped = value.lstrip("-")
    n = len(value) - len(stripped)
    return f"{_ESCAPE_BOUNDARY}{n}{_ESCAPE_BOUNDARY}{stripped}"


def decode_secret_value(value: str) -> str:
    """
    Inverse of encode_secret_value. Applied automatically by secret_callback for
    options built through this module (see get_cli_arg). Call this explicitly only
    if you parse a kwargs_to_cli_args()/get_cli_arg() args string some other way,
    e.g. with a different parser or in another program/language.
    """
    if not value.startswith(_ESCAPE_BOUNDARY):
        return value
    _, n, rest = value.split(_ESCAPE_BOUNDARY, 2)
    return "-" * int(n) + rest


def secret_callback(ctx: click.Context, param: click.Option, value: Any):
    """
    Here we try to get the secret option value from an environment variable.
    The reason for doing this in the callback instead of using a callable default is
    that we don't want the default to be displayed in the prompt. There seems to
    be no way of altering this behaviour.
    """
    if value == SECRET_DISPLAY:
        # Derived from param.opts[0] (the CLI flag itself) rather than param.name, so this
        # keeps tracking the flag actually shown to the user (e.g. in --help) even for an
        # option declared directly via make_option_secret with a custom internal name.
        # Hyphens are converted to underscores because POSIX environment variable names
        # can't contain them (#173) - matching the underscored names documented in
        # user-guide.md, e.g. "--db-password" -> "DB_PASSWORD".
        envar_name = param.opts[0][2:].upper().replace("-", "_")
        return os.environ.get(envar_name)
    if isinstance(value, str):
        return decode_secret_value(value)
    return value


class StdTags(Flag):
    DB = auto()
    BFS = auto()
    ONPREM = auto()
    SAAS = auto()
    SLC = auto()


class StdParams(Enum):
    """
    Standard option keys.
    """

    bucketfs_name = (StdTags.BFS | StdTags.ONPREM, auto())
    bucketfs_host = (StdTags.BFS | StdTags.ONPREM, auto())
    bucketfs_port = (StdTags.BFS | StdTags.ONPREM, auto())
    bucketfs_use_https = (StdTags.BFS | StdTags.ONPREM, auto())
    bucketfs_user = (StdTags.BFS | StdTags.ONPREM, auto())
    bucketfs_password = (StdTags.BFS | StdTags.ONPREM, auto())
    bucket = (StdTags.BFS | StdTags.ONPREM, auto())
    saas_url = (StdTags.DB | StdTags.BFS | StdTags.SAAS, auto())
    saas_account_id = (StdTags.DB | StdTags.BFS | StdTags.SAAS, auto())
    saas_database_id = (StdTags.DB | StdTags.BFS | StdTags.SAAS, auto())
    saas_database_name = (StdTags.DB | StdTags.BFS | StdTags.SAAS, auto())
    saas_token = (StdTags.DB | StdTags.BFS | StdTags.SAAS, auto())
    path_in_bucket = (StdTags.BFS | StdTags.ONPREM | StdTags.SAAS, auto())
    container_file = (StdTags.SLC, auto())
    version = (StdTags.SLC, auto())
    dsn = (StdTags.DB | StdTags.ONPREM, auto())
    db_user = (StdTags.DB | StdTags.ONPREM, auto())
    db_password = (StdTags.DB | StdTags.ONPREM, auto())
    language_alias = (StdTags.SLC, auto())
    schema = (StdTags.DB | StdTags.ONPREM | StdTags.SAAS, auto())
    ssl_cert_path = (StdTags.DB | StdTags.ONPREM, auto())
    ssl_client_cert_path = (StdTags.DB | StdTags.ONPREM, auto())
    ssl_client_private_key = (StdTags.DB | StdTags.ONPREM, auto())
    use_ssl_cert_validation = (StdTags.DB | StdTags.BFS | StdTags.ONPREM, auto())
    upload_container = (StdTags.SLC, auto())
    alter_system = (StdTags.SLC, auto())
    allow_override = (StdTags.SLC, auto())
    wait_for_completion = (StdTags.SLC, auto())
    deploy_timeout_minutes = (StdTags.SLC, auto())
    display_progress = (StdTags.SLC, auto())

    def __init__(self, tags: StdTags, value):
        self.tags = tags


StdParamOrName = StdParams | str


def _get_param_name(std_param: StdParamOrName) -> str:
    return std_param if isinstance(std_param, str) else std_param.name


"""
Standard options defined in the form of key-value pairs, where key is the option's
StaParam key and the value is a kwargs for creating the click.Options(...).
"""
_std_options: dict[StdParams, dict[str, Any]] = {
    StdParams.bucketfs_name: {"type": str},
    StdParams.bucketfs_host: {"type": str},
    StdParams.bucketfs_port: {"type": int},
    StdParams.bucketfs_use_https: {"type": bool, "default": False},
    StdParams.bucketfs_user: {"type": str},
    StdParams.bucketfs_password: {"type": str, "hide_input": True},
    StdParams.bucket: {"type": str},
    StdParams.saas_url: {"type": str, "default": "https://cloud.exasol.com"},
    StdParams.saas_account_id: {"type": str, "hide_input": True},
    StdParams.saas_database_id: {"type": str, "hide_input": True},
    StdParams.saas_database_name: {"type": str},
    StdParams.saas_token: {"type": str, "hide_input": True},
    StdParams.path_in_bucket: {"type": str, "default": ""},
    StdParams.container_file: {"type": click.Path(exists=True, file_okay=True)},
    StdParams.version: {"type": str, "expose_value": False},
    StdParams.dsn: {"type": str},
    StdParams.db_user: {"type": str},
    StdParams.db_password: {"type": str, "hide_input": True},
    StdParams.language_alias: {"type": str},
    StdParams.schema: {"type": str, "default": ""},
    StdParams.ssl_cert_path: {"type": str, "default": ""},
    StdParams.ssl_client_cert_path: {"type": str, "default": ""},
    StdParams.ssl_client_private_key: {"type": str, "default": ""},
    StdParams.use_ssl_cert_validation: {"type": bool, "default": True},
    StdParams.upload_container: {"type": bool, "default": True},
    StdParams.alter_system: {"type": bool, "default": True},
    StdParams.allow_override: {"type": bool, "default": False},
    StdParams.wait_for_completion: {"type": bool, "default": True},
    StdParams.deploy_timeout_minutes: {"type": int, "default": 10},
    StdParams.display_progress: {"type": bool, "default": True},
}


def make_option_secret(option_params: dict[str, Any], prompt: str) -> None:
    """
    Makes an option "secret" in the way that its input is not leaked to the
    terminal. The option can be either a standard or a user defined.

    Parameters:
    option_params:
        Option properties.
    prompt:
        The prompt text for this option.
    """
    option_params["hide_input"] = True
    option_params["prompt"] = prompt
    option_params["prompt_required"] = False
    option_params["default"] = SECRET_DISPLAY
    option_params["callback"] = secret_callback


def get_opt_name(std_param: StdParamOrName) -> str:
    """
    Converts a parameter, e.g. "db_user", to the option definition, e.g. "--db-user"
    """
    std_param_name = _get_param_name(std_param)
    return f'--{std_param_name.replace("_", "-")}'


def get_bool_opt_name(std_param: StdParamOrName) -> str:
    """
    Converts a boolean parameter, e.g. "alter_system", to the option definition, e.g.
    "--alter-system/--no-alter-system".
    """
    std_param_name = _get_param_name(std_param)
    opt_name = std_param_name.replace("_", "-")
    return f"--{opt_name}/--no-{opt_name}"


def is_secret_param(std_param: StdParamOrName) -> bool:
    """
    True if std_param is a StdParams member defined with hide_input=True in
    _std_options. A plain string name is only considered secret if it happens to match
    the name of such a StdParams member; any other string name can never be secret,
    since it has no entry in _std_options.

    Note: this only reflects the default hide_input in _std_options, not any
    hide_input a caller passed directly to create_std_option or via
    select_std_options(override=...). get_cli_arg (the only caller) is only ever
    given a param name, not the click.Option that was actually constructed, so it
    has no way to see such an override.
    """
    if isinstance(std_param, StdParams):
        member = std_param
    elif std_param in StdParams.__members__:
        member = StdParams[std_param]
    else:
        return False
    if member not in _std_options:
        return False
    return bool(_std_options[member].get("hide_input", False))


def create_std_option(std_param: StdParamOrName, **kwargs) -> click.Option:
    """
    Creates a Click option.

    Parameters:
    std_param:
        The option's StdParam key or its name. If the name is provided it doesn't have
        to be the name of a defined StdParams. It can be an arbitrary identifier which
        will be passed to the callback function of the CLI command in the kwargs.
    kwargs:
        The option properties.
    """
    param_decls = [
        get_bool_opt_name(std_param) if kwargs.get("type") is bool else get_opt_name(std_param)
    ]
    if kwargs.get("hide_input", False):
        make_option_secret(kwargs, prompt=_get_param_name(std_param).replace("_", " "))
    return click.Option(param_decls, **kwargs)


@no_type_check
def select_std_options(
    tags: StdTags | list[StdTags] | str,
    exclude: StdParams | list[StdParams] | None = None,
    override: dict[StdParams, dict[str, Any]] | None = None,
    formatters: dict[StdParams, ParameterFormatters] | None = None,
) -> list[click.Option]:
    """
    Selects all or a subset of the defined standard Click options.

    Parameters:
    tags:
        A flag or a list of flags that define the option selection criteria. Each flag
        is a combination of the StdTags. An option gets selected if it's StdParams.tags
        property includes any of the provided flags.
        If the tags is the string "all" all the standard options will be selected.
    exclude:
        An option or a list of options that should not to be included in the output even
        though they match the tags criteria.
    override:
        A dictionary of standard options with overridden properties
    formatters:
        A dictionary, with each key being a source CLI parameter,
        see docstring of class ParameterFormatters.

        Each value is an instance of ParameterFormatters representing a single
        or multiple destination parameters to be updated based on the source
        parameter's value.

        If a particular destination parameter D depends on multiple source
        parameters S1, S2, ..., then the Click API will iterate through the
        source parameters Si and update D multiple times.
    """
    if not isinstance(tags, list) and not isinstance(tags, str):
        tags = [tags]
    if exclude is None:
        exclude = []
    elif not isinstance(exclude, list):
        exclude = [exclude]
    override = override or {}
    formatters = formatters or {}

    def options_filter(std_param: StdParams) -> bool:
        return any(tag in std_param.tags for tag in tags) and std_param not in exclude

    def option_params(std_param: StdParams) -> dict[str, Any]:
        return override[std_param] if std_param in override else _std_options[std_param]

    if tags == "all":
        filtered_params = _std_options
    else:
        filtered_params = filter(options_filter, _std_options)
    return [
        create_std_option(std_param, **option_params(std_param), callback=formatters.get(std_param))
        for std_param in filtered_params
    ]


def get_cli_arg(std_param: StdParamOrName, param_value: Any) -> str:
    """
    Makes a CLI args string from an option and its value.
    An option can be given as either an StdParams or its string name.
    For boolean values the args string takes the form --option-name/--no-option-name.
    A non-boolean value is quoted with shlex.quote, so the returned string can be
    split back into args with shlex.split (as click.testing.CliRunner.invoke does for
    a string args) regardless of what characters the value contains.

    For a "secret" (hide_input) standard parameter, click's parser can't tell an
    option value starting with "-"/"--" apart from the option being given with no
    value at all, since such an option allows omitting its value (which is how it
    lets its value be entered interactively instead) - this holds no matter how the
    option and its value are joined in the returned string. To avoid that, such a
    value is encoded with encode_secret_value before being put on the command line.

    This is decoded back automatically only if the resulting args string is parsed by
    a click.Option built through this module (create_std_option, select_std_options,
    make_option_secret), since decoding happens in their shared secret_callback. A
    caller who instead parses this string themselves, or hands it to a different
    program/language, must call decode_secret_value explicitly to recover the
    original value.
    """

    option_name = _get_param_name(std_param).replace("_", "-")
    if isinstance(param_value, bool):
        return f"--{option_name}" if param_value else f"--no-{option_name}"
    str_value = str(param_value)
    if is_secret_param(std_param):
        str_value = encode_secret_value(str_value)
    return f"--{option_name} {shlex.quote(str_value)}"


def kwargs_to_cli_args(**kwargs) -> str:
    """
    Rolls out kwargs into a CLI arg string.
    """
    return " ".join(get_cli_arg(k, v) for k, v in kwargs.items())


def check_params(
    std_params: StdParamOrName | list[StdParamOrName | list[StdParamOrName]],
    param_kwargs: dict[str, Any],
) -> bool:
    """
    Checks if the kwargs contain specified StdParams keys. The intention is to verify
    if the options provided by the user via a CLI are sufficient to perform a certain
    operation. An option value of any type other than boolean is considered valid if
    converted to boolean it evaluates to True. All boolean values found in the kwargs
    are considered valid.

    The keys shall be provided in a list that represents a logical expression in the
    Conjunctive Normal Form (CNF). Any logical expression can be transformed to the CNF
    using De Morgan's rules. An expression is presented as a 2d list with conjunctions
    (AND) at the first level and disjunctions (OR) at the second. For example the list
    [a, [b, c], d] reads as "a AND (b OR c) AND d".

    Parameters:
    std_params:
        Required options. This can be either a single option or a list of options
        representing a logical expression. An option can be given as either an StdParams
        or its string name.
    param_kwargs:
        A dictionary of provided values (kwargs).
    """

    def check_param(std_param: StdParamOrName | list[StdParamOrName]) -> bool:
        if isinstance(std_param, list):
            return any(check_param(std_param_i) for std_param_i in std_param)
        std_param_name = _get_param_name(std_param)
        return (std_param_name in param_kwargs) and (
            isinstance(param_kwargs[std_param_name], bool) or bool(param_kwargs[std_param_name])
        )

    if isinstance(std_params, list):
        return all(check_param(std_param) for std_param in std_params)
    return check_param(std_params)
