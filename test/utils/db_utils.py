import textwrap

from pyexasol import ExaConnection


def create_schema(pyexasol_connection: ExaConnection, schema: str, open_test_schema: bool = True):
    pyexasol_connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
    pyexasol_connection.execute(f'CREATE SCHEMA "{schema}";')
    if not open_test_schema:
        pyexasol_connection.execute("CLOSE SCHEMA;")


def open_schema(pyexasol_connection: ExaConnection, schema: str):
    pyexasol_connection.execute(f'OPEN SCHEMA "{schema}";')


def assert_udf_running(pyexasol_connection: ExaConnection, language_alias: str, schema: str):
    pyexasol_connection.execute(
        textwrap.dedent(
            f"""
        CREATE OR REPLACE {language_alias} SCALAR SCRIPT {schema}."TEST_UDF"()
        RETURNS BOOLEAN AS
        def run(ctx):
            return True
        /
        """
        )
    )
    result = pyexasol_connection.execute(f'SELECT {schema}."TEST_UDF"()').fetchall()
    assert result[0][0] is True
