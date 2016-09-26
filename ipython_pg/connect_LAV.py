import os
import psycopg2

PORT = 5433
HOST = "lav-fileserver"
SSLCERT = os.path.join(os.path.expanduser("~"), "subnetz.org.crt")


def connect(user=os.getlogin(), port=PORT, host=HOST, 
            password=None, dbname=None, sslcert=SSLCERT):
    """ Connect to a Postgres database server """

    if password is None:
        import getpass
        password = getpass.getpass("password for {}@{}:{}:"
                                   .format(user, host, port))

    if dbname is None:
        dbname = input("db name:")

    args = locals()
    args = {k: v for k, v in args.items()
            if k in ("password", "host", "port", "sslcert", "user", "dbname")}
    dsn = ("{}='{}'".format(*a) for a in args.items())
    dsn = " ".join(dsn)

    return psycopg2.connect(dsn)