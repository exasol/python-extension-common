from typing import Any, no_type_check
import os
import re
from enum import Flag, Enum, auto
import click


class ParameterFormatters:
    """
    Class facilitating customization of the cli.

    The idea is that some of the cli parameters can be programmatically customized based
    on values of other parameters and externally supplied formatters. For example a specialized
    version of the cli may want to provide its own url. Furthermore, this url will depend on
    the user supplied parameter called "version". The solution is to set a formatter for the
    url, for instance "http://my_stuff/{version}/my_data". If the user specifies non-empty version
    parameter the url will be fully formed.

    A formatter may include more than one parameter. In the previous example the url could,
    for instance, also include a username: "http://my_stuff/{version}/{user}/my_data".

    Note that customized parameters can only be updated in a callback function. There is no
    way to inject them directly into the cli. Also, the current implementation doesn't perform
    the update if the value of the parameter dressed with the callback is None.
    """
    def __init__(self):
        self._formatters: dict[str, str] = {}

    def __call__(self, ctx: click.Context, param: click.Parameter, value: Any | None) -> Any | None:

        def update_parameter(parameter_name: str, formatter: str) -> None:
            param_formatter = ctx.params.get(parameter_name, formatter)
            if param_formatter:
                # Enclose in double curly brackets all other parameters in the formatting string,
                # to avoid the missing parameters' error. Below is an example of a formatter string
                # before and after applying the regex, assuming the current parameter is 'version'.
                # 'something-with-{version}/tailored-for-{user}' => 'something-with-{version}/tailored-for-{{user}}'
                # We were looking for all occurrences of a pattern '{some_name}', where some_name is not version.
                pattern = r'\{(?!' + (param.name or '') + r'\})\w+\}'
                param_formatter = re.sub(pattern, lambda m: f'{{{m.group(0)}}}', param_formatter)
                kwargs = {param.name: value}
                ctx.params[parameter_name] = param_formatter.format(**kwargs)

        if value is not None:
            for prm_name, prm_formatter in self._formatters.items():
                update_parameter(prm_name, prm_formatter)

        return value

    def set_formatter(self, custom_parameter_name: str, formatter: str) -> None:
        """ Sets a formatter for a customizable parameter. """
        self._formatters[custom_parameter_name] = formatter

    def clear_formatters(self):
        """ Deletes all formatters, mainly for testing purposes. """
        self._formatters.clear()


# This text will be displayed instead of the actual value for a "secret" option.
SECRET_DISPLAY = '***'


def secret_callback(ctx: click.Context, param: click.Option, value: Any):
    """
    Here we try to get the secret option value from an environment variable.
    The reason for doing this in the callback instead of using a callable default is
    that we don't want the default to be displayed in the prompt. There seems to
    be no way of altering this behaviour.
    """
    if value == SECRET_DISPLAY:
        envar_name = param.opts[0][2:].upper()
        return os.environ.get(envar_name)
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

    def __init__(self, tags: StdTags, value):
        self.tags = tags


"""
Standard options defined in the form of key-value pairs, where key is the option's
StaParam key and the value is a kwargs for creating the click.Options(...).
"""
_std_options = {
    StdParams.bucketfs_name: {'type': str},
    StdParams.bucketfs_host: {'type': str},
    StdParams.bucketfs_port: {'type': int},
    StdParams.bucketfs_use_https: {'type': bool, 'default': False},
    StdParams.bucketfs_user: {'type': str},
    StdParams.bucketfs_password: {'type': str, 'hide_input': True},
    StdParams.bucket: {'type': str},
    StdParams.saas_url: {'type': str, 'default': 'https://cloud.exasol.com'},
    StdParams.saas_account_id: {'type': str, 'hide_input': True},
    StdParams.saas_database_id: {'type': str, 'hide_input': True},
    StdParams.saas_database_name: {'type': str},
    StdParams.saas_token: {'type': str, 'hide_input': True},
    StdParams.path_in_bucket: {'type': str},
    StdParams.container_file: {'type': click.Path(exists=True, file_okay=True)},
    StdParams.version: {'type': str, 'expose_value': False},
    StdParams.dsn: {'type': str},
    StdParams.db_user: {'type': str},
    StdParams.db_password: {'type': str, 'hide_input': True},
    StdParams.language_alias: {'type': str},
    StdParams.schema: {'type': str, 'default': ''},
    StdParams.ssl_cert_path: {'type': str, 'default': ''},
    StdParams.ssl_client_cert_path: {'type': str, 'default': ''},
    StdParams.ssl_client_private_key: {'type': str, 'default': ''},
    StdParams.use_ssl_cert_validation: {'type': bool, 'default': True},
    StdParams.upload_container: {'type': bool, 'default': True},
    StdParams.alter_system: {'type': bool, 'default': True},
    StdParams.allow_override: {'type': bool, 'default': False},
    StdParams.wait_for_completion: {'type': bool, 'default': True}
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
    option_params['hide_input'] = True
    option_params['prompt'] = prompt
    option_params['prompt_required'] = False
    option_params['default'] = SECRET_DISPLAY
    option_params['callback'] = secret_callback


def create_std_option(std_param: StdParams, **kwargs) -> click.Option:
    """
    Creates a Click option.

    Parameters:
    std_param:
        The option's StdParam key.
    kwargs:
        The option properties.
    """
    option_name = std_param.name.replace('_', '-')
    if kwargs.get('type') == bool:
        param_decls = [f'--{option_name}/--no-{option_name}']
    else:
        param_decls = [f'--{option_name}']
    if kwargs.get('hide_input', False):
        make_option_secret(kwargs, prompt=std_param.name.replace('_', ' '))
    return click.Option(param_decls, **kwargs)


@no_type_check
def select_std_options(tags: StdTags | list[StdTags] | str,
                       exclude: StdParams | list[StdParams] | None = None,
                       override: dict[StdParams, dict[str, Any]] | None = None,
                       formatters: dict[StdParams, ParameterFormatters] | None = None
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

    if tags == 'all':
        filtered_params = _std_options
    else:
        filtered_params = filter(options_filter, _std_options)
    return [create_std_option(std_param, **option_params(std_param),
                              callback=formatters.get(std_param))
            for std_param in filtered_params]


def check_params(std_params: StdParams | list[StdParams | list[StdParams]],
                 param_kwargs: dict[str, Any]) -> bool:
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
        representing a logical expression.
    param_kwargs:
        A dictionary of provided values (kwargs).
    """
    def check_param(std_param: StdParams | list[StdParams]) -> bool:
        if isinstance(std_param, list):
            return any(check_param(std_param_i) for std_param_i in std_param)
        return ((std_param.name in param_kwargs) and
                (isinstance(param_kwargs[std_param.name], bool) or
                 bool(param_kwargs[std_param.name])))

    if isinstance(std_params, list):
        return all(check_param(std_param) for std_param in std_params)
    return check_param(std_params)
