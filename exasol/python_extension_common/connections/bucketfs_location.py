from typing import Any
from enum import Enum, auto
import json
import pyexasol     # type: ignore
import exasol.bucketfs as bfs   # type: ignore
from exasol.saas.client.api_access import get_database_id   # type: ignore

from exasol.python_extension_common.cli.std_options import StdParams, check_params
from exasol.python_extension_common.connections.pyexasol_connection import open_pyexasol_connection


class DBType(Enum):
    onprem = auto()
    saas = auto()


def _infer_db_type(bfs_params: dict[str, Any]) -> DBType:

    if check_params([StdParams.bucketfs_host, StdParams.bucketfs_port,
                     StdParams.bucket, StdParams.bucketfs_user, StdParams.bucketfs_password],
                    bfs_params):
        return DBType.onprem
    elif check_params([StdParams.saas_url, StdParams.saas_account_id, StdParams.saas_token,
                       [StdParams.saas_database_id, StdParams.saas_database_name]], bfs_params):
        return DBType.saas

    raise ValueError(
        'Incomplete parameter list. Please either provide the parameters ['
        f'{StdParams.bucketfs_host.name}, {StdParams.bucketfs_port.name}, '
        f'{StdParams.bucketfs_name.name}, {StdParams.bucket.name}, '
        f'{StdParams.bucketfs_user.name}, {StdParams.bucketfs_password.name}] '
        f'for an On-Prem database or [{StdParams.saas_url.name}, '
        f'{StdParams.saas_account_id.name}, {StdParams.saas_database_id.name} or '
        f'{StdParams.saas_database_name.name}, {StdParams.saas_token.name}] for a '
        'SaaS database.'
    )


def _convert_onprem_bfs_params(bfs_params: dict[str, Any]) -> dict[str, Any]:

    net_service = ('https' if bfs_params.get(StdParams.bucketfs_use_https.name, True)
                   else 'http')
    url = (f"{net_service}://"
           f"{bfs_params[StdParams.bucketfs_host.name]}:"
           f"{bfs_params[StdParams.bucketfs_port.name]}")
    return {
        'backend': bfs.path.StorageBackend.onprem,
        'url': url,
        'username': bfs_params[StdParams.bucketfs_user.name],
        'password': bfs_params[StdParams.bucketfs_password.name],
        'service_name': bfs_params.get(StdParams.bucketfs_name.name),
        'bucket_name': bfs_params[StdParams.bucket.name],
        'verify': bfs_params.get(StdParams.use_ssl_cert_validation.name, True),
        'path': bfs_params.get(StdParams.path_in_bucket.name, '')
    }


def _convert_saas_bfs_params(bfs_params: dict[str, Any]) -> dict[str, Any]:

    saas_url = bfs_params[StdParams.saas_url.name]
    saas_account_id = bfs_params[StdParams.saas_account_id.name]
    saas_token = bfs_params[StdParams.saas_token.name]
    saas_database_id = (bfs_params.get(StdParams.saas_database_id.name) or
                        get_database_id(
                            host=saas_url,
                            account_id=saas_account_id,
                            pat=saas_token,
                            database_name=bfs_params[StdParams.saas_database_name.name]
                        ))
    return {
        'backend': bfs.path.StorageBackend.saas,
        'url': saas_url,
        'account_id': saas_account_id,
        'database_id': saas_database_id,
        'pat': saas_token,
        'path': bfs_params.get(StdParams.path_in_bucket.name, '')
    }


def _to_json_str(bucketfs_params: dict[str, Any], selected: list[str]) -> str:
    filtered_kwargs = {k: v for k, v in bucketfs_params.items()
                       if (k in selected) and (v is not None)}
    return json.dumps(filtered_kwargs)


def create_bucketfs_location(**kwargs) -> bfs.path.PathLike:
    """
    Creates a BucketFS PathLike object using the data provided in the kwargs. These
    can be parameters for the BucketFS either On-Prem or SaaS database. The parameters
    should correspond to the CLI options defined in the cli/std_options.py.

    Raises a ValueError if the provided parameters are insufficient for either
    On-Prem or SaaS cases.
    """

    db_type = _infer_db_type(kwargs)
    if db_type == DBType.onprem:
        return bfs.path.build_path(**_convert_onprem_bfs_params(kwargs))
    else:
        return bfs.path.build_path(**_convert_saas_bfs_params(kwargs))


def _write_bucketfs_conn_object(pyexasol_connection: pyexasol.ExaConnection,
                                conn_name: str,
                                conn_to: str,
                                conn_user: str,
                                conn_password: str) -> None:

    query = (f"CREATE OR REPLACE  CONNECTION {conn_name} "
             f"TO '{conn_to}' "
             f"USER '{conn_user}' "
             f"IDENTIFIED BY '{conn_password}'")
    pyexasol_connection.execute(query)


def create_bucketfs_conn_object_onprem(pyexasol_connection: pyexasol.ExaConnection,
                                       conn_name: str,
                                       bucketfs_params: dict[str, Any]) -> None:
    conn_to = _to_json_str(bucketfs_params, [
        'backend', 'url', 'service_name', 'bucket_name', 'path', 'verify'])
    conn_user = _to_json_str(bucketfs_params, ['username'])
    conn_password = _to_json_str(bucketfs_params, ['password'])

    _write_bucketfs_conn_object(pyexasol_connection, conn_name,
                                conn_to, conn_user, conn_password)


def create_bucketfs_conn_object_saas(pyexasol_connection: pyexasol.ExaConnection,
                                     conn_name: str,
                                     bucketfs_params: dict[str, Any]) -> None:
    conn_to = _to_json_str(bucketfs_params, ['backend', 'url', 'path'])
    conn_user = _to_json_str(bucketfs_params, ['account_id', 'database_id'])
    conn_password = _to_json_str(bucketfs_params, ['pat'])

    _write_bucketfs_conn_object(pyexasol_connection, conn_name,
                                conn_to, conn_user, conn_password)


def create_bucketfs_conn_object(conn_name: str, **kwargs) -> None:
    """
    """
    with open_pyexasol_connection(**kwargs) as pyexasol_connection:
        db_type = _infer_db_type(kwargs)
        if db_type == DBType.onprem:
            create_bucketfs_conn_object_onprem(pyexasol_connection, conn_name,
                                               _convert_onprem_bfs_params(kwargs))
        else:
            create_bucketfs_conn_object_onprem(pyexasol_connection, conn_name,
                                               _convert_saas_bfs_params(kwargs))


def create_bucketfs_location_from_conn_object(conn_obj) -> bfs.path.PathLike:
    """
    Create BucketFS PathLike object using data contained in the provided connection object.
    """

    bfs_params = json.loads(conn_obj.address)
    bfs_params.update(json.loads(conn_obj.user))
    bfs_params.update(json.loads(conn_obj.password))
    return bfs.path.build_path(**bfs_params)
