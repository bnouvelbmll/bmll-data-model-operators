"""Bounded read-only warehouse connections for inventory and delivery observers.

Credentials come from the operator environment. Snowflake accepts a mounted key
or PEM supplied by a Kubernetes Secret; neither is included in reports.
"""
import os


def connect(backend):
    if backend == 'databricks':
        from databricks import sql
        return sql.connect(server_hostname=os.environ['DATABRICKS_HOST'].removeprefix('https://'),
                           http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
    import snowflake.connector
    account = os.environ['SNOWFLAKE_ACCOUNT']
    region = os.getenv('SNOWFLAKE_REGION', '')
    if region and '.' not in account and '-' not in account:
        account += '.' + region
    kw = dict(account=account, user=os.environ['SNOWFLAKE_USER'], login_timeout=30,
              session_parameters={'STATEMENT_TIMEOUT_IN_SECONDS': 180, 'QUERY_TAG': 'data-model-observer'})
    for key in ('WAREHOUSE', 'ROLE'):
        if os.getenv('SNOWFLAKE_' + key): kw[key.lower()] = os.environ['SNOWFLAKE_' + key]
    if os.getenv('SNOWFLAKE_PRIVATE_KEY_PEM'):
        from cryptography.hazmat.primitives import serialization
        password = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        key = serialization.load_pem_private_key(os.environ['SNOWFLAKE_PRIVATE_KEY_PEM'].encode(), password=password.encode() if password else None)
        kw.update(authenticator='SNOWFLAKE_JWT', private_key=key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    elif os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'):
        kw.update(authenticator='SNOWFLAKE_JWT', private_key_file=os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'], private_key_file_pwd=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'))
    else:
        kw['password'] = os.environ['SNOWFLAKE_PASSWORD']
    return snowflake.connector.connect(**kw)
