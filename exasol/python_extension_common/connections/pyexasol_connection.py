import exasol.saas.client.api_access as saas_api  # type: ignore
import pyexasol  # type: ignore

from exasol.python_extension_common.cli.std_options import (
    StdParams,
    check_params,
)
from exasol.python_extension_common.deployment.language_container_deployer import (
    get_websocket_sslopt,
)


def open_pyexasol_connection(**kwargs) -> pyexasol.ExaConnection:
    """
    Creates a database connection to either On-Prem or SaaS database, depending on
    the provided parameters. The parameters should correspond to the CLI options
    defined in the cli/std_options.py.

    Raises a ValueError if the provided parameters are insufficient for either
    On-Prem or SaaS connections.
    """

    # Fix the compatibility issue
    if ("db_pass" in kwargs) and not (StdParams.db_password.name in kwargs):
        kwargs[StdParams.db_password.name] = kwargs["db_pass"]

    # Infer where the database is - On-Prem or SaaS.
    if check_params([StdParams.dsn, StdParams.db_user, StdParams.db_password], kwargs):
        connection_params = {
            "dsn": kwargs[StdParams.dsn.name],
            "user": kwargs[StdParams.db_user.name],
            "password": kwargs[StdParams.db_password.name],
        }
    elif check_params(
        [
            StdParams.saas_url,
            StdParams.saas_account_id,
            StdParams.saas_token,
            [StdParams.saas_database_id, StdParams.saas_database_name],
        ],
        kwargs,
    ):
        connection_params = saas_api.get_connection_params(
            host=kwargs[StdParams.saas_url.name],
            account_id=kwargs[StdParams.saas_account_id.name],
            database_id=kwargs.get(StdParams.saas_database_id.name),
            database_name=kwargs.get(StdParams.saas_database_name.name),
            pat=kwargs[StdParams.saas_token.name],
        )
    else:
        raise ValueError(
            "Incomplete parameter list. Please either provide the parameters "
            f"[{StdParams.dsn.name}, {StdParams.db_user.name}, {StdParams.db_password.name}] "
            f"for an On-Prem database or [{StdParams.saas_url.name}, {StdParams.saas_account_id.name}, "
            f"{StdParams.saas_database_id.name} or {StdParams.saas_database_name.name}, "
            f"{StdParams.saas_token.name} saas_token] for a SaaS database."
        )

    websocket_sslopt = get_websocket_sslopt(
        use_ssl_cert_validation=kwargs.get(StdParams.use_ssl_cert_validation.name, True),
        ssl_trusted_ca=kwargs.get(StdParams.ssl_cert_path.name, ""),
        ssl_client_certificate=kwargs.get(StdParams.ssl_client_cert_path.name, ""),
        ssl_private_key=kwargs.get(StdParams.ssl_client_private_key.name, ""),
    )

    return pyexasol.connect(
        **connection_params,
        schema=kwargs.get(StdParams.schema.name, ""),
        encryption=True,
        websocket_sslopt=websocket_sslopt,
        compression=True,
    )
