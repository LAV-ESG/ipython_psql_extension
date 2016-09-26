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


from contextlib import contextmanager
import string
import re
from IPython.core.magic import (Magics, line_magic, line_cell_magic,
                                magics_class)
from IPython.display import HTML
import psycopg2


@magics_class
class pgMagics(Magics):

    def __init__(self, shell, default_host="localhost", default_port=5432,
                 default_sslcert=None, default_user=None, auto_commit=True):
        """Create a new pgMagic instance.

        Arguments:
            shell {IpythonInteractiveShell} -- IPython interpreter.
            default_host {str} -- address to connect to if not otherwise
                                  specified (default: localhost).
            default_port {int} -- port to connect to when not oterhwise
                                  specified (default: 5432).
            default_sslcert {str} -- ssl-certificate to use if not
                                     otherwise specified. If None, do not
                                     use any (default: None).
            default_user {str} -- username to login with if not otherwise
                                  specified (default: system login).
            auto_commit {bool} -- if True, commit when queries with row-count
                                  -1 are executed (queries that create objects
                                  such as tables and view) (default: True).
        """
        super(pgMagics, self).__init__(shell)
        self.dbconn = None
        self.last_query = None
        self.default_host = str(default_host)
        self.default_port = int(default_port)
        self.default_sslcert = (None if default_sslcert is None
                                else str(default_sslcert))
        self.default_user = (None if default_user is None
                             else str(default_user))
        self.auto_commit=bool(auto_commit)

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
        """Close connection to the database server."""
        if hasattr(self.dbconn, 'close'):
            self.dbconn.close()
        self.dbconn = None

    def _dbconn(self):
        if self.dbconn is None:
            raise RuntimeError("need to connect first (type '%pg_connect')\n")
        return self.dbconn

    @line_magic
    def pg_rollback(self, arg):
        """ Resets the connection object after an error """
        self._dbconn().rollback()

    @line_magic
    def pg_commit(self, arg=None):
        """End current transaction by an explicit commit."""
        self._dbconn().commit()

    @line_magic
    def pg_cursor(self, arg=None):
        """ Return a new cursor object, for more direct access """
        return self._dbconn().cursor()

    @contextmanager
    def catch_errors(self):
        try:
            yield
        except Exception as e:
            self.shell.write_err("ERROR: {}\n".format(str(e)))
            self.dbconn.rollback()

    def cur_report(self, cur):
        """Run 'sql' against the database."""
        if cur.rowcount == 0:
            self.shell.write("WARNING: query did not match any row\n")
        elif cur.rowcount == -1:
            self.shell.write("SUCCESS: query did not return any data\n")
            if self.auto_commit:
                self.pg_commit()
        else:
            self.shell.write("SUCCES: matched {} rows\n".format(cur.rowcount))

    def query(self, sql):
        if "${" in sql:
            sql = string.Template(sql)
            lcs = get_ipython().ev("locals()")
            sql = sql.substitute(**lcs)

        with self.catch_errors():
            cur = self.pg_cursor()
            cur.execute(sql)
            self.cur_report(cur)
        return cur

    @line_cell_magic
    def pg_sql(self, line, cell=None):
        """ Query the database.

        Executes an SQL query against the open database connection and provides
        access to the results. It can be be used both as a cell and line magic.

        Usage as a line-magic:
           [<varname> = ]%pg_sql <sql>

        Usage as cell magic:
           %%pg_sql [<varname>]
           <sql>

        When used as a line-magic, the cursor used to query the database is
        returned. It an either be stored in a variable, or will just be sent
        to the default output (in which case it is accessible thorugh _*).

        As a cell-magic, when used without the optional <varname> argument,
        results are returned as an HTML table in a IPthon.display.HTML
        instance. Note that, in order not to destabilize the browser, only the
        first 500 rows are displayed. If however <varname> is specified, then
        no output is provided, but instead the cursor used to query the database
        is made available as a local variable of name <varname>.

        Within <sql>, the values of variables declared within the IPython
        session can be accessed by using string.Template sytnax. E.g.:

            In [1]: N = 2

            In [2]: %pg_sql select * from tbl where id = ${N}

        is equivalent to:

            In [3]: %pg_sql select * from tbl where id = 2
        """
        if cell is None:
            output = None
            query = str(line)
        else:
            output = str(line)
            query = str(cell)

        cur = self.query(query)

        if output is None or len(output.strip()) == 0:
            if cell is None:
                return cur
            return self.display_cur_as_table(cur)

        self.shell.write(" cursor object as '{}'\n".format(output))
        self.shell.push({output: cur})

    @line_magic
    def pg_first(self, sql):
        """Run 'sql' and return its first row.

        This line-magic behaves identical the %pg_sql line-magic in every way,
        except that it returns only the first row instead of the entire cursor.
        """
        cur = self.query(str(sql))
        return cur.fetchone()

    @line_cell_magic
    def pg_one(self, line, cell=None):
        """Run 'sql' and return its first row.

        Line magic usage:
            %pg_one <sql>

        This can be run both as a line and cell magic. Just as the line magic
        %pg_first, it only returns the first row. Additionally though, if there
        is only one column, it will return its value (instead of a one-element
        tuple).

        Cell magic usage:
            %%pg_one [<varname>]
            <sql>

        Similar to %pg_sql it can also be used as a cell-magic. If <varname>
        is specified, then the result is made available as local variable.
        Contrary to $pg_sql, thre is no HTML output though (but variable
        substitution works).
        """
        if cell is None:
            output = None
            query = str(line.strip())
        else:
            output = str(line.strip())
            query = str(cell.strip())

        row = self.pg_first(query)
        if row is not None:
            row = row[0] if len(row) == 1 else row

        if output:
            self.shell.push({output: row})
            self.shell.write(" result stored under '{}'\n".format(output))
        return row

    def display_cur_as_table(self, cur, row_limit=500):
        """ Displays the results in the given cursor object as HTML table """

        if cur.rowcount < 1:
            return  # no results to display

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

        for i, row in enumerate(cur):
            if i >= row_limit:
                self.shell.write("WARNING: displaying only the first {} rows"
                                 .format(row_limit))
                break
            html.append("<tr>")
            for cell in row:
                html.append("<td>{}</td>".format(str(cell)))
            html.append("</tr>")
        html.append("</tbody>")
        html.append("</table>")
        return HTML("".join(html))

