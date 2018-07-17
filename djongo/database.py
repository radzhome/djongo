from pymongo import MongoClient
import urllib

# Patch for auth
#def connect(**kwargs):
#    return MongoClient(**kwargs)
def connect(**kwargs):
    username = kwargs.get('username')
    qpassword = urllib.parse.quote_plus(kwargs.get('password', ''))
    host = kwargs.get('host')
    port = kwargs.get('port')
    dbname = kwargs.get('dbname')
    if username:
        url = "mongodb://{}:{}@{}:{}/".format(username, qpassword, host, port)
    else:
        url = "mongodb://{}:{}/".format(host, port)
    
    if dbname:
        url += dbname
    return MongoClient(url)


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
