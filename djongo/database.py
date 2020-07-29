from pymongo import MongoClient
import urllib
from urllib import parse


# Patch for auth, ssl
def connect(**kwargs):
    username = kwargs.get('username')
    qpassword = urllib.parse.quote_plus(kwargs.get('password', ''))
    host = kwargs.get('host')
    hosts = kwargs.get('hosts')
    host_srv = kwargs.get('host_srv')
    port = kwargs.get('port')
    dbname = kwargs.get('dbname')
    replica_set = kwargs.get('replicaset')
    retry_writes = 'true' if kwargs.get('retryWrites') in [1, '1', True, 'true'] else 'false'
    read_preference = kwargs.get('readPreference')  # primary, secondary, nearest etc..
    write_concern = kwargs.get('w')  # 0 - n
    ssl_option = 'true' if kwargs.get('ssl') in [1, '1', True, 'true'] else 'false'
    auth_source = kwargs.get('authSource')

    options = kwargs.get('options')
    connect_timeout = int(options.get('connect_timeout') or 10) * 1000  # Convert sec to ms
    socket_timeout = int(options.get('timeout') or 45) * 1000  # Convert sec to ms
    server_selection_timeout = int(options.get('server_selection_timeout') or 10) * 1000  # Convert sec to ms
    wait_queue_timeout = int(options.get('wait_queue_timeout') or 45) * 1000  # Convert sec to ms

    # Add params that are set to url
    params = {
        'ssl': ssl_option,
        'replicaSet': replica_set,
        'authSource': auth_source,
        'retryWrites': retry_writes,
        'readPreference': read_preference,
        'w': write_concern,
        'connectTimeoutMS': connect_timeout,
        'socketTimeoutMS': socket_timeout,
        'serverSelectionTimeoutMS': server_selection_timeout,
        'waitQueueTimeoutMS': wait_queue_timeout,
    }

    # Complete conn string now
    # http://www.mongoing.com/docs/reference/connection-string.html#standard-connection-string-format

    srv_str = ''
    if host_srv:
        srv_str = '+srv'
        host = host_srv
    elif hosts:
        host = ["{}:{}".format(h['host'], h['port']) for h in hosts]
        host = ','.join(host)
    else:
        host = "{}:{}".format(host, port)

    if username:
        url = "mongodb{}://{}:{}@{}/".format(srv_str, username, qpassword, host)
    else:
        url = "mongodb{}://{}/".format(srv_str, host)

    if dbname:
        url += dbname

        
    # https://stackoverflow.com/questions/31030307/why-is-pymongo-3-giving-serverselectiontimeouterror
    if ssl_option:
        # kwargs.update({'connect': False})
        # By default, PyMongo is configured to require a certificate from the server when TLS is enabled. This disables.
        params.update({'ssl_cert_reqs': 'CERT_NONE'})
        
    params = {key: value for key, value in params.items() if value is not None}
    url += '?' + parse.urlencode(params)


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
