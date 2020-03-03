from pymongo import MongoClient
import urllib
from urllib import parse


# Patch for auth, ssl
def connect(**kwargs):
    username = kwargs.get('username')
    qpassword = urllib.parse.quote_plus(kwargs.get('password', ''))
    host = kwargs.get('host')
    hosts = kwargs.get('hosts')
    port = kwargs.get('port')
    dbname = kwargs.get('dbname')
    replica_set = kwargs.get('replicaset')
    retry_writes = kwargs.get('retryWrites')
    write_concern = kwargs.get('w')
    ssl_option = 'true' if kwargs.get('ssl') in [1, '1', True, 'true'] else ''
    auth_source = kwargs.get('authSource')

    # Complete conn string now
    # http://www.mongoing.com/docs/reference/connection-string.html#standard-connection-string-format
    if hosts:
        host = ','.join(["{}:{}".format(h['host'], h['port']) for h in hosts])
    else:
        host = "{}:{}".format(host, port)

    if username:
        url = "mongodb://{}:{}@{}/".format(username, qpassword, host)
    else:
        url = "mongodb://{}/".format(host)

    if dbname:
        url += dbname

    # Add params that are set to url
    params = {
        'ssl': ssl_option,
        'replicaSet': replica_set,
        'authSource': auth_source,
        'retryWrites': retry_writes,
        'w': write_concern,
    }
    params = {key: value for key, value in params.items() if value is not None}
    url += '?' + parse.urlencode(params)

    kwargs = {}
    if ssl_option:
        # By default, PyMongo is configured to require a certificate from the server when TLS is enabled. This disables.
        kwargs = {'ssl_cert_reqs': 'CERT_NONE'}

    return MongoClient(url, **kwargs)


class Error(Exception):  # NOQA: StandardError undefined on PY3
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
