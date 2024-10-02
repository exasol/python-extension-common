import pytest
import pyexasol

from exasol.python_extension_common.cli.std_options import StdParams
from exasol.python_extension_common.connections.pyexasol_connection import open_pyexasol_connection


def validate_connection(conn: pyexasol.ExaConnection) -> None:
    res = conn.execute('SELECT SESSION_ID FROM SYS.EXA_ALL_SESSIONS;').fetchall()
    assert res


def test_open_pyexasol_connection_onprem(use_onprem,
                                         backend_aware_onprem_database,
                                         exasol_config):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    kwargs = {
        StdParams.dsn.name: f'{exasol_config.host}:{exasol_config.port}',
        StdParams.db_user.name: exasol_config.username,
        StdParams.db_password.name: exasol_config.password
    }
    with open_pyexasol_connection(**kwargs) as conn:
        validate_connection(conn)


def test_open_pyexasol_connection_saas_db_id(use_saas,
                                             saas_host,
                                             saas_pat,
                                             saas_account_id,
                                             backend_aware_saas_database_id):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    kwargs = {
        StdParams.saas_url.name: saas_host,
        StdParams.saas_account_id.name: saas_account_id,
        StdParams.saas_database_id.name: backend_aware_saas_database_id,
        StdParams.saas_token.name: saas_pat
    }
    with open_pyexasol_connection(**kwargs) as conn:
        validate_connection(conn)


def test_open_pyexasol_connection_saas_db_name(use_saas,
                                               saas_host,
                                               saas_pat,
                                               saas_account_id,
                                               backend_aware_saas_database_id,
                                               database_name):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    kwargs = {
        StdParams.saas_url.name: saas_host,
        StdParams.saas_account_id.name: saas_account_id,
        StdParams.saas_database_name.name: database_name,
        StdParams.saas_token.name: saas_pat
    }
    with open_pyexasol_connection(**kwargs) as conn:
        validate_connection(conn)


def test_open_pyexasol_connection_error():
    kwargs = {
        StdParams.dsn.name: 'my_dsn',
        StdParams.saas_url.name: 'my_saas_url',
    }
    with pytest.raises(ValueError):
        open_pyexasol_connection(**kwargs)
