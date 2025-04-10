import pyexasol
import pytest

from exasol.python_extension_common.cli.std_options import StdParams
from exasol.python_extension_common.connections.pyexasol_connection import (
    open_pyexasol_connection,
)


def validate_connection(conn: pyexasol.ExaConnection) -> None:
    res = conn.execute("SELECT SESSION_ID FROM SYS.EXA_ALL_SESSIONS;").fetchall()
    assert res


def test_open_pyexasol_connection_onprem(use_onprem, onprem_db_params):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    with open_pyexasol_connection(**onprem_db_params) as conn:
        validate_connection(conn)


def test_open_pyexasol_connection_saas_db_id(use_saas, saas_params_id):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    with open_pyexasol_connection(**saas_params_id) as conn:
        validate_connection(conn)


def test_open_pyexasol_connection_saas_db_name(use_saas, saas_params_name):
    if not use_saas:
        pytest.skip("The test is not configured to use SaaS.")

    with open_pyexasol_connection(**saas_params_name) as conn:
        validate_connection(conn)


def test_open_pyexasol_connection_error():
    kwargs = {
        StdParams.dsn.name: "my_dsn",
        StdParams.saas_url.name: "my_saas_url",
    }
    with pytest.raises(ValueError):
        open_pyexasol_connection(**kwargs)
