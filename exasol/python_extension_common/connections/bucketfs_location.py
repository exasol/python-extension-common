import json
from dataclasses import dataclass
from enum import (
    Enum,
    auto,
)
from typing import Any

import exasol.bucketfs as bfs  # type: ignore
import pyexasol  # type: ignore
from exasol.saas.client.api_access import get_database_id  # type: ignore

from exasol.python_extension_common.cli.std_options import (
    StdParams,
    check_params,
)
from exasol.python_extension_common.connections.pyexasol_connection import (
    open_pyexasol_connection,
)


class _Backend(Enum):
    onprem = auto()
    saas = auto()


def _infer_backend(bfs_params: dict[str, Any]) -> _Backend:
    """
    Infers the backend from the provided dictionary of CLI parameters.
    Raises a ValueError if the collection of CLI parameters is insufficient to access
    the BucketFS on either of the backends.
    """

    if check_params(
        [
            StdParams.bucketfs_host,
            StdParams.bucketfs_port,
            StdParams.bucket,
            StdParams.bucketfs_user,
            StdParams.bucketfs_password,
        ],
        bfs_params,
    ):
        return _Backend.onprem
    elif check_params(
        [
            StdParams.saas_url,
            StdParams.saas_account_id,
            StdParams.saas_token,
            [StdParams.saas_database_id, StdParams.saas_database_name],
        ],
        bfs_params,
    ):
        return _Backend.saas

    raise ValueError(
        "Incomplete parameter list. Please either provide the parameters ["
        f"{StdParams.bucketfs_host.name}, {StdParams.bucketfs_port.name}, "
        f"{StdParams.bucketfs_name.name}, {StdParams.bucket.name}, "
        f"{StdParams.bucketfs_user.name}, {StdParams.bucketfs_password.name}] "
        f"for an On-Prem database or [{StdParams.saas_url.name}, "
        f"{StdParams.saas_account_id.name}, {StdParams.saas_database_id.name} or "
        f"{StdParams.saas_database_name.name}, {StdParams.saas_token.name}] for a "
        "SaaS database."
    )


def _convert_onprem_bfs_params(bfs_params: dict[str, Any]) -> dict[str, Any]:
    """
    Converts OnPrem BucketFS parameters from the CLI format to the format expected
    by the exasol.bucketfs.path.build_path.
    """

    net_service = "https" if bfs_params.get(StdParams.bucketfs_use_https.name, True) else "http"
    url = (
        f"{net_service}://"
        f"{bfs_params[StdParams.bucketfs_host.name]}:"
        f"{bfs_params[StdParams.bucketfs_port.name]}"
    )
    return {
        "backend": bfs.path.StorageBackend.onprem.name,
        "url": url,
        "username": bfs_params[StdParams.bucketfs_user.name],
        "password": bfs_params[StdParams.bucketfs_password.name],
        "service_name": bfs_params.get(StdParams.bucketfs_name.name),
        "bucket_name": bfs_params[StdParams.bucket.name],
        "verify": bfs_params.get(StdParams.use_ssl_cert_validation.name, True),
        "path": bfs_params.get(StdParams.path_in_bucket.name, ""),
    }


def _convert_saas_bfs_params(bfs_params: dict[str, Any]) -> dict[str, Any]:
    """
    Converts SaaS BucketFS parameters from the CLI format to the format expected
    by the exasol.bucketfs.path.build_path.
    """

    saas_url = bfs_params[StdParams.saas_url.name]
    saas_account_id = bfs_params[StdParams.saas_account_id.name]
    saas_token = bfs_params[StdParams.saas_token.name]
    saas_database_id = bfs_params.get(StdParams.saas_database_id.name) or get_database_id(
        host=saas_url,
        account_id=saas_account_id,
        pat=saas_token,
        database_name=bfs_params[StdParams.saas_database_name.name],
    )
    return {
        "backend": bfs.path.StorageBackend.saas.name,
        "url": saas_url,
        "account_id": saas_account_id,
        "database_id": saas_database_id,
        "pat": saas_token,
        "path": bfs_params.get(StdParams.path_in_bucket.name, ""),
    }


def create_bucketfs_location(**kwargs) -> bfs.path.PathLike:
    """
    Creates a BucketFS PathLike object using the data provided in the kwargs. These
    can be parameters for the BucketFS at either On-Prem or SaaS database. The input
    parameters should correspond to the CLI options defined in the cli/std_options.py.

    Raises a ValueError if the provided parameters are insufficient for either
    On-Prem or SaaS cases.
    """

    db_type = _infer_backend(kwargs)
    if db_type == _Backend.onprem:
        return bfs.path.build_path(**_convert_onprem_bfs_params(kwargs))
    else:
        return bfs.path.build_path(**_convert_saas_bfs_params(kwargs))


@dataclass
class ConnectionInfo:
    """
    This is not a connection object. It's just a structure to keep together the data
    required for creating a BucketFs connection object. Useful for testing.
    """

    address: str
    user: str
    password: str


def _to_json_str(bucketfs_params: dict[str, Any], selected: list[str]) -> str:
    filtered_kwargs = {
        k: v for k, v in bucketfs_params.items() if (k in selected) and (v is not None)
    }
    return json.dumps(filtered_kwargs)


def write_bucketfs_conn_object(
    pyexasol_connection: pyexasol.ExaConnection, conn_name: str, conn_obj: ConnectionInfo
) -> None:

    query = (
        f"CREATE OR REPLACE  CONNECTION {conn_name} "
        f"TO '{conn_obj.address}' "
        f"USER '{conn_obj.user}' "
        f"IDENTIFIED BY '{conn_obj.password}'"
    )
    pyexasol_connection.execute(query)


def create_bucketfs_conn_object_onprem(
    pyexasol_connection: pyexasol.ExaConnection, conn_name: str, bucketfs_params: dict[str, Any]
) -> None:
    """
    Creates in the database a connection object encapsulating the BucketFS parameters
    for an OnPrem backend.

    Parameters:
    pyexasol_connection:
        DB connection.
    conn_name:
        Name for the connection object.
    bucketfs_params:
        OnPrem BucketFS parameters in the format of the exasol.bucketfs.path.build_path.
    """
    conn_to = _to_json_str(
        bucketfs_params, ["backend", "url", "service_name", "bucket_name", "path", "verify"]
    )
    conn_user = _to_json_str(bucketfs_params, ["username"])
    conn_password = _to_json_str(bucketfs_params, ["password"])

    write_bucketfs_conn_object(
        pyexasol_connection, conn_name, ConnectionInfo(conn_to, conn_user, conn_password)
    )


def create_bucketfs_conn_object_saas(
    pyexasol_connection: pyexasol.ExaConnection, conn_name: str, bucketfs_params: dict[str, Any]
) -> None:
    """
    Creates in the database a connection object encapsulating the BucketFS parameters
    for a SaaS backend.

    Parameters:
    pyexasol_connection:
        DB connection.
    conn_name:
        Name for the connection object.
    bucketfs_params:
        SaaS BucketFS parameters in the format of the exasol.bucketfs.path.build_path.
    """
    conn_to = _to_json_str(bucketfs_params, ["backend", "url", "path"])
    conn_user = _to_json_str(bucketfs_params, ["account_id", "database_id"])
    conn_password = _to_json_str(bucketfs_params, ["pat"])

    write_bucketfs_conn_object(
        pyexasol_connection, conn_name, ConnectionInfo(conn_to, conn_user, conn_password)
    )


def create_bucketfs_conn_object(conn_name: str, **kwargs) -> None:
    """
    Creates in the database a connection object encapsulating the provided BucketFS
    parameters. These can be parameters for either On-Prem or SaaS database. They
    should correspond to the CLI options defined in the cli/std_options.py.

    Raises a ValueError if the provided parameters are insufficient for either
    On-Prem or SaaS cases.
    """
    with open_pyexasol_connection(**kwargs) as pyexasol_connection:
        db_type = _infer_backend(kwargs)
        if db_type == _Backend.onprem:
            create_bucketfs_conn_object_onprem(
                pyexasol_connection, conn_name, _convert_onprem_bfs_params(kwargs)
            )
        else:
            create_bucketfs_conn_object_saas(
                pyexasol_connection, conn_name, _convert_saas_bfs_params(kwargs)
            )


def create_bucketfs_location_from_conn_object(conn_obj) -> bfs.path.PathLike:
    """
    Creates a BucketFS PathLike object using data contained in the provided connection
    object.
    """

    bfs_params = json.loads(conn_obj.address)
    bfs_params.update(json.loads(conn_obj.user))
    bfs_params.update(json.loads(conn_obj.password))
    return bfs.path.build_path(**bfs_params)
