from pymongo import MongoClient
import urllib


# Patch for auth
def connect(**kwargs):
    username = kwargs.get('username')
    qpassword = urllib.parse.quote_plus(kwargs.get('password', ''))
    host = kwargs.get('host')
    hosts = kwargs.get('hosts')
    port = kwargs.get('port')
    dbname = kwargs.get('dbname')
    replicaset = kwargs.get('replicaset')

    # Complete conn string now
    # http://api.mongodb.com/python/current/examples/high_availability.html
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

    if replicaset:
        url += "?replicaSet={}".format(replicaset)

    return MongoClient(url)


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
