import exasol.bucketfs as bfs   # type: ignore
from exasol.saas.client.api_access import get_database_id   # type: ignore

from exasol.python_extension_common.cli.std_options import StdParams, check_params


def create_bucketfs_location(**kwargs) -> bfs.path.PathLike:
    """
    Creates a BucketFS PathLike object using the data provided in the kwargs. These
    can be parameters for the BucketFS either On-Prem or SaaS database. The parameters
    should correspond to the CLI options defined in the cli/std_options.py.

    Raises a ValueError if the provided parameters are insufficient for either
    On-Prem or SaaS cases.
    """

    path_in_bucket = kwargs.get(StdParams.path_in_bucket.name, '')

    # Infer where the database is - on-prem or SaaS.
    if check_params([StdParams.bucketfs_host, StdParams.bucketfs_port,
                     StdParams.bucket, StdParams.bucketfs_user, StdParams.bucketfs_password],
                    kwargs):
        net_service = ('https' if kwargs.get(StdParams.bucketfs_use_https.name, True)
                       else 'http')
        url = (f"{net_service}://"
               f"{kwargs[StdParams.bucketfs_host.name]}:"
               f"{kwargs[StdParams.bucketfs_port.name]}")
        return bfs.path.build_path(
            backend=bfs.path.StorageBackend.onprem,
            url=url,
            username=kwargs[StdParams.bucketfs_user.name],
            password=kwargs[StdParams.bucketfs_password.name],
            service_name=kwargs.get(StdParams.bucketfs_name.name),
            bucket_name=kwargs[StdParams.bucket.name],
            verify=kwargs.get(StdParams.use_ssl_cert_validation.name, True),
            path=path_in_bucket
        )
    elif check_params([StdParams.saas_url, StdParams.saas_account_id, StdParams.saas_token,
                       [StdParams.saas_database_id, StdParams.saas_database_name]], kwargs):
        saas_url = kwargs[StdParams.saas_url.name]
        saas_account_id = kwargs[StdParams.saas_account_id.name]
        saas_token = kwargs[StdParams.saas_token.name]
        saas_database_id = (kwargs.get(StdParams.saas_database_id.name) or
                            get_database_id(
                                host=saas_url,
                                account_id=saas_account_id,
                                pat=saas_token,
                                database_name=kwargs[StdParams.saas_database_name.name]
                            ))
        return bfs.path.build_path(backend=bfs.path.StorageBackend.saas,
                                   url=saas_url,
                                   account_id=saas_account_id,
                                   database_id=saas_database_id,
                                   pat=saas_token,
                                   path=path_in_bucket)

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
