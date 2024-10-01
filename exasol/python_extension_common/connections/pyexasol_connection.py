from typing import Any
import pyexasol     # type: ignore
import exasol.saas.client.api_access as saas_api    # type: ignore

from exasol.python_extension_common.cli.std_options import StdParams
from exasol.python_extension_common.deployment.language_container_deployer import get_websocket_sslopt


def check_params(std_params: list[StdParams | list[StdParams]], param_kwargs: dict[str, Any]) -> bool:
    def check_param(std_param: StdParams | list[StdParams]) -> bool:
        if isinstance(std_param, list):
            return any(check_param(std_param_i) for std_param_i in std_param)
        return (std_param.name in param_kwargs) and bool(param_kwargs[std_param.name])

    return all(check_param(std_param) for std_param in std_params)


def open_pyexasol_connection(**kwargs) -> pyexasol.ExaConnection:
    """
    Creates a database connections object, either in an On-Prem or SaaS database,
    depending on the provided parameters. The provided parameters should correspond
    to the cli options defined in the cli/std_options.py.

    Raises a ValueError if the provided parameters are sufficient for neither On-Prem
    nor SaaS connections.
    """

    # Fix the compatibility issue
    if ('db_pass' in kwargs) and not (StdParams.db_password.name in kwargs):
        kwargs[StdParams.db_password.name] = kwargs['db_pass']

    # Infer where the database is - On-Prem or SaaS.
    if check_params([StdParams.dsn, StdParams.db_user, StdParams.db_password], kwargs):
        connection_params = {
            'dsn': kwargs[StdParams.dsn.name],
            'user': kwargs[StdParams.db_user.name],
            'password': kwargs[StdParams.db_password.name]
        }
    elif check_params([StdParams.saas_url, StdParams.saas_account_id, StdParams.saas_token,
                       [StdParams.saas_database_id, StdParams.saas_database_name]], kwargs):
        connection_params = saas_api.get_connection_params(
            host=kwargs[StdParams.saas_url.name],
            account_id=kwargs[StdParams.saas_account_id.name],
            database_id=kwargs.get(StdParams.saas_database_id.name),
            database_name=kwargs.get(StdParams.saas_database_name.name),
            pat=kwargs[StdParams.saas_token.name]
        )
    else:
        raise ValueError('Incomplete parameter list. '
                         'Please either provide the parameters [dsn, db_user, db_pass] '
                         'for an On-Prem database or [saas_url, saas_account_id, '
                         'saas_database_id or saas_database_name, saas_token] '
                         'for a SaaS database.')

    websocket_sslopt = get_websocket_sslopt(
        use_ssl_cert_validation=kwargs.get(StdParams.use_ssl_cert_validation.name, True),
        ssl_trusted_ca=kwargs.get(StdParams.ssl_cert_path.name, ''),
        ssl_client_certificate=kwargs.get(StdParams.ssl_client_cert_path.name, ''),
        ssl_private_key=kwargs.get(StdParams.ssl_client_private_key.name, '')
    )

    return pyexasol.connect(
        **connection_params,
        schema=kwargs.get(StdParams.schema.name, ''),
        encryption=True,
        websocket_sslopt=websocket_sslopt,
        compression=True
    )
