""" Simplifies using Postgres from IPython

    :author: Gil Georges <gil.georges@lav.mavt.ethz.ch>

    Usage:
      1. %pg_connect [<DSN>] to connect to a Postgres server using the
         postgres database source name specification <DSN>
      2. %pg_disconnect to disconnect when done
      3. %pg_cursor to get a cursor instance to access the database
      4. %pg_rollback to rollback the connection object
      5. the line/cell magic %pg_sql to perform queries against the
         database. It can be used as line-magic:
            %pg_sql [<sql>]
         or cell-magic:
            %%pg_sql [<varname>]
            <sql>
         where <sql> is a pgsql query, and <varname> is a valid python
         variable name. If provided (only for use with the cell-magic),
         the cursor holding the query's results will be made available
         within the IPython session under the variable <varname>. If
         ommitted (only when used with the cell-magic), the results
         are reproduced as an HTML table. Be careful here, very large
         result sets may crash the display tool (console/browser).

    Installation:

    """


from IPython.core.magic import (Magics, line_magic, line_cell_magic,
                                magics_class)
from IPython.display import HTML

import psycopg2
import re


@magics_class
class pgMagics(Magics):

    def __init__(self, shell, default_host="localhost", default_port=5432,
                 default_sslcert=None, default_user=None):
        super(pgMagics, self).__init__(shell)
        self.dbconn = None
        self.last_query = None
        self.default_host = str(default_host)
        self.default_port = int(default_port)
        self.default_sslcert = (None if default_sslcert is None
                                else str(default_sslcert))
        self.default_user = (None if default_user is None
                             else str(default_user))

    @line_magic
    def pg_connect(self, arg):
        """ Connect to a Postgres database server

        The inline-magic "%pg_connect [DSN]" connects to a Postgres
        server using psycopg2 and the provided 'DSN'. The connection
        stays open in the background (managed by this extension), until
        closed using '%pg_close'.

        Example:
           %pg_connect host='localhost' user='root' dbname='postgres'
        """

        try:
            args = re.split(" +", arg)
            args = (re.match(r"^([\w]+) *= *(['\"])?(.*)(?(2)\2|)$", a)
                    for a in args)
            args = (a for a in args if hasattr(a, 'groups'))
            args = (a.groups() for a in args)
            args = {a[0]: a[2] for a in args}
        except ValueError:
            self.shell.write_err("ERROR: invalid DSN string")
            return

        if "user" not in args:
            args["user"] = (input("user:") if self.default_user is None
                            else self.default_user)

        args.setdefault("port", self.default_port)
        args.setdefault("host", self.default_host)

        if "password" not in args:
            import getpass
            args["password"] = getpass.getpass("password for {}@{}:{}:"
                                               .format(args["user"],
                                                       args["host"],
                                                       args["port"]))

        if "dbname" not in args:
            args["dbname"] = input("dbname:")

        if self.default_sslcert is not None:
            args.setdefault("sslcert", self.default_sslcert)

        dsn = ("{}='{}'".format(*a) for a in args.items())
        dsn = " ".join(dsn)

        try:

            self.dbconn = psycopg2.connect(dsn)
        except psycopg2.OperationalError as e:
            self.shell.write_err("ERROR: unable to connect! Got {}"
                                 .format(str(e)))
            return

        self.shell.write("SUCCESS: connected to {}".format(args["host"]))

    @line_magic
    def pg_disconnect(self, arg):
        if hasattr(self.dbconn, 'close'):
            self.dbconn.close()
        self.dbconn = None

    @line_magic
    def pg_rollback(self, arg):
        """ Resets the connection object after an error """
        if not hasattr(self.dbconn, 'rollback'):
            print("ERROR: need to connect first")
            return
        self.dbconn.rollback()

    @line_cell_magic
    def pg_sql(self, line, cell=None):
        """ Queries the database

        Usage:
           %pg_sql [<varname>]

        Arguments:
           <varname>   Variable under which the cursors object shall be
                       available later in the IPython document.
                       If ommitted, return the results as a table. """

        if cell is None:
            output = None
            query = str(line)
        else:
            output = str(line)
            query = str(cell)

        if self.dbconn is None:
            self.shell.write_err("ERROR: need to connect first "
                                 "(using '%pg_connect')")
            return

        cur = self.pg_cursor(None)

        try:
            cur.execute(query)
        except psycopg2.ProgrammingError as e:
            self.shell.write_err("ERROR: {}".format(str(e)))
            cur.close()
            self.dbconn.rollback()
            return

        self.last_query = query
        self.shell.write("SUCCES: matched {} rows".format(cur.rowcount))

        if output is None or len(output.strip()) == 0:
            return self.display_cur_as_table(cur)

        self.shell.write(" cursor object available under '{}'".format(output))
        self.shell.push({output: cur})

    @line_magic
    def pg_cursor(self, arg):
        """ Returns a new cursor object, for more direct access """
        return self.dbconn.cursor()

    def display_cur_as_table(self, cur):
        """ Displays the results in the given cursor object as HTML table """

        html = ['<table width="100%">']

        html.append("<thead>")
        html.append("<tr>")
        for col in cur.description:
            html.append("<th>{}</th>".format(col[0]))
        html.append("</tr>")
        html.append("</thead>")

        html.append("<tbody>")

        if not hasattr(cur, 'description') or cur.rowcount == 0:
            html.append('<tr>')
            html.append('<td colspan="{}" align="center">'
                        .format(len(cur.description)))
            html.append('(no results to display)')
            html.append('</td></tr>')

        for row in cur:
            html.append("<tr>")
            for cell in row:
                html.append("<td>{}</td>".format(str(cell)))
            html.append("</tr>")
        html.append("</tbody>")
        html.append("</table>")
        return HTML("".join(html))

