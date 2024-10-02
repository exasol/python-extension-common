import pytest
import exasol.bucketfs as bfs

from exasol.python_extension_common.cli.std_options import StdParams
from exasol.python_extension_common.connections.bucketfs_location import create_bucketfs_location


def validate_bfs_path(bfs_path: bfs.path.PathLike) -> None:
    file_content = b'A rose by any other name would smell as sweet.'
    bfs_path.write(file_content)
    data_back = b''.join(bfs_path.read())
    bfs_path.rm()
    assert data_back == file_content


def test_create_bucketfs_location_onprem(use_onprem,
                                         backend_aware_onprem_database,
                                         bucketfs_config):
    if not use_onprem:
        pytest.skip("The test is not configured to use ITDE.")

    kwargs = {
        StdParams.bucketfs_user.name: bucketfs_config.username,
        StdParams.bucketfs_password.name: bucketfs_config.password,
        StdParams.bucketfs_name.name: 'bfsdefault',
        StdParams.bucket.name: 'default',
        StdParams.use_ssl_cert_validation.name: False,
        StdParams.path_in_bucket.name: 'test_path'
    }
    bfs_path = create_bucketfs_location(**kwargs)
    validate_bfs_path(bfs_path)


def test_create_bucketfs_location_saas_db_id(use_saas,
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
    bfs_path = create_bucketfs_location(**kwargs)
    validate_bfs_path(bfs_path)


def test_create_bucketfs_location_saas_db_name(use_saas,
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
    bfs_path = create_bucketfs_location(**kwargs)
    validate_bfs_path(bfs_path)


def test_create_bucketfs_location_error():
    kwargs = {
        StdParams.bucketfs_name.name: 'bfsdefault',
        StdParams.bucket.name: 'default',
        StdParams.saas_url.name: 'my_saas_url',
    }
    with pytest.raises(ValueError):
        create_bucketfs_location(**kwargs)
