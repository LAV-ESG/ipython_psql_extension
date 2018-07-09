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
import getpass
from IPython.core.magic import (Magics, line_magic, line_cell_magic,
                                magics_class)
from IPython.display import HTML
import psycopg2
import psycopg2.extensions
import pandas as pd

SQL_SCHEMAS = ("select nspname as name, "
               "coalesce(pg_catalog.obj_description(oid), '(no description)')"
               " as schema_description "
               "from pg_catalog.pg_namespace "
               "where nspname !~ 'pg_(temp|toast|catalog).*' "
               "and nspname != 'information_schema'")

SQL_TABLES = ("SELECT c.relname as table_name, "
              "coalesce(pg_catalog.obj_description(c.oid), '(no description)')"
              " as table_description "
              "FROM pg_catalog.pg_class as c "
              "LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
              "WHERE n.nspname = '{}' and c.relkind in ('r', 'v')")

SQL_COLUMNS = ("select column_name as name,"
               "case when precision > 0 then "
               "(udt_name || '(' || precision || ')') "
               "else (udt_name || '()') end as type, "
               "pg_catalog.col_description(foo.oid, foo.ordinal_position) "
               "as description from ("
               "select udt_catalog, udt_name, ordinal_position, column_name, "
               "coalesce(character_maximum_length, numeric_precision, "
               "datetime_precision, 0) as precision, "
               "table_name, ("
               "select ('\"' || table_schema || '\".\"' || table_name || '\"'"
               ")::regclass::oid) as oid from information_schema.columns "
               "where table_schema = '{}' and table_name='{}') as foo;")

@magics_class
class pgMagics(Magics):

    def __init__(self, shell, default_host="localhost", default_port=5432,
                 default_sslcert=None, default_user=None, auto_commit=True,
                 disable_postgis_integration=False, disable_green_mode=False):
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
        self.auto_commit = bool(auto_commit)
        self.postgis_integration = not bool(disable_postgis_integration)
        self.green_mode = not bool(disable_green_mode)

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

        if self.green_mode:
            from . import green_mode
            green_mode.activate()

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

        if "dbname" not in args:
            args["dbname"] = input("dbname:")

        if self.default_sslcert is not None:
            args.setdefault("sslcert", self.default_sslcert)

        dsn = ("{}='{}'".format(*a) for a in args.items())
        dsn = " ".join(dsn)

        # quick fiX:
        try:
            self.dbconn = psycopg2.connect(dsn)
        except psycopg2.OperationalError as e:
            if "password" in args:
                self.shell.write_err("ERROR: unable to connect! Got {}"
                                     .format(str(e)))
                return

            args["password"] = getpass.getpass("password for {}@{}:{}:"
                                               .format(args["user"],
                                                       args["host"],
                                                       args["port"]))
            dsn = ("{}='{}'".format(*a) for a in args.items())
            dsn = " ".join(dsn)
            try:
                self.dbconn = psycopg2.connect(dsn)
            except psycopg2.OperationalError as e:
                self.shell.write_err("ERROR: unable to connect! Got {}"
                                     .format(str(e)))
                return

        self.shell.write("SUCCESS: connected to {}".format(args["host"]))

        if self.postgis_integration:
            try:
                from . import postgis_integration
            except ImportError:
                self.shell.write("WARNING: The PostGIS extension has been "
                                 "disabled, because 'shapely' is not "
                                 "installed. You only need this if you work "
                                 "with geo-spatial information. You can "
                                 "suppress this error message by "
                                 "disabling the postgis extension.")
            else:
                try:
                    postgis_integration.activate(conn=self.dbconn)
                    self.shell.write("\n  PostGIS integration enabled")
                except postgis_integration.PostGISnotInstalled:
                    pass

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
        """Reset the connection object after an error."""
        self._dbconn().rollback()

    @line_magic
    def pg_commit(self, arg=None):
        """End current transaction by an explicit commit."""
        self._dbconn().commit()

    @line_magic
    def pg_cursor(self, arg=None):
        """Return a new cursor object, for more direct access."""
        return self._dbconn().cursor()

    @line_magic
    def pg_connection(self, arg=None):
        """Return psycopg2 connection object."""
        return self._dbconn()

    @contextmanager
    def catch_errors(self):
        """General purpose context manager for database access."""
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

    def _python_tpl(self, sql):
        def _adapt(v):
            v = self.shell.ev(expr)
            return str(psycopg2.extensions.adapt(v))

        rxp = re.compile(r"(?<!\$)\${([^}]*)}")
        txt = str(sql)
        for expr in rxp.findall(sql):
            txt = txt.replace("${%s}" % expr, _adapt(expr))
        return txt


    def query(self, sql):
        """Query the database and perform variable substitution."""
        if "${" in sql:
            sql = self._python_tpl(sql)

        with self.catch_errors():
            cur = self.pg_cursor()
            cur.execute(sql)
            self.cur_report(cur)
        return cur


    @line_cell_magic
    def pg_sql(self, line, cell=None):
        """Query the database.

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
        no output is provided, but instead the cursor that queried the database
        is made available as a local variable of name <varname>.

        Within <sql>, the values of variables declared within the IPython
        session can be accessed by using string.Template sytnax. E.g.:

            In [1]: N = 2

            In [2]: %pg_sql select * from tbl where id = ${N}

        is equivalent to:

            In [3]: %pg_sql select * from tbl where id = 2
        """
        query, output = _line_cell_prep(line, cell)
        cur = self.query(query)

        if output:
            self.shell.write(" cursor object as '{}'\n".format(output))
            self.shell.push({output: cur})
            return

        if cell is None:
            return cur

        return self.display_cur_as_table(cur)


    @line_cell_magic
    def pg_pd(self, line, cell=None):
        """Query the database.

        Executes an SQL query against the open database connection and returns
        the results as pandas dataframe.
        """
        query, output = _line_cell_prep(line, cell)
        cur = self.query(query)
        dta = pd.DataFrame([r for r in cur],
                           columns=[c.name for c in cur.description])
        cur.close()

        if output:
            self.shell.write(" results stored as '{}'\n".format(output))
            self.shell.push({output: dta})
            return

        return dta

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
        query, output = _line_cell_prep(line, cell)
        row = self.pg_first(query)
        if row is not None:
            row = row[0] if len(row) == 1 else row

        if output:
            self.shell.push({output: row})
            self.shell.write(" result stored under '{}'\n".format(output))
        return row

    @line_cell_magic
    def pg_tuple(self, line, cell=None):
        """Return each column as a tuple.

        Line magic usage:
            [<var1>[, <var2>]... = ] %pg_tuple <sql>

        Cell magic usage:
            %%pg_tuple [<var1>[, <var2>]...]
            <sql>

        This magic returns a tuple of tuples, where each tuple corresponds
        to the data of a column of the result table. If just one output
        variable ("<var1>") is specified, the tuple of tuples will be availabe
        there. If multiple <var> are specified (as many as there are columns),
        the tuple of tuple will be expanded onto them.
        """
        query, args = _line_cell_prep(line, cell)
        args = re.split(", *", args) if args else []
        cur = self.query(query)

        if not args:
            return tuple(zip(*cur))

        if len(args) == 1:
            self.shell.push({args[0]: tuple(zip(*cur))})
            self.shell.write(" result stored under '{}'\n".format(args[0]))
            return

        if len(args) < len(cur.description):
            raise ValueError("too many values to unpack (expected {})"
                             .format(len(args)))
        if len(args) > len(cur.description):
            raise ValueError("too few values to unpack (expected {})"
                             .format(len(args)))

        for arg, values in zip(args, zip(*cur)):
            self.shell.push({arg: tuple(values)})
        output = ", ".join("'{}'".format(s) for s in args)
        self.shell.write(" results stored under \n".format(output))

    def display_cur_as_table(self, cur, row_limit=500):
        """Display the results in the given cursor object as HTML table.

        Arguments:
            cur {cursor} -- results to be displayed
            row_limit {int} -- number of rows to display (default: 500)

        Returns:
            IPython.display.HTML -- HTML table representation
        """
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

    @line_magic
    def pg_info(self, obj):
        """Retrieve meta-information on database contents.

        Usage:
            %pg_info [<name>]

        If used without arguments, returns the names of all schemas in the
        databse. If <name> is a schema name, returns the names of all tables
        in that schema. And if <name> is a table name (needs to be prefixed
        with the schema if not on the search path), returns all columns in
        that table. """
        obj = str(obj).replace('"', '').replace("'", r"\'")
        if not obj:
            sql = SQL_SCHEMAS
        elif "." in obj:
            obj = obj.split(".")
            sql = SQL_COLUMNS.format(obj[0], obj[1])
        else:
            sql = SQL_TABLES.format(obj)

        with self.catch_errors():
            cur = self.pg_cursor()
            cur.execute(sql)

        return self.display_cur_as_table(cur)


def _line_cell_prep(line, cell=None):
    """Default logic for line-cell magis."""
    if cell is None:
        args = None
        query = str(line)
    else:
        args = str(line).strip()
        args = None if len(args) == 0 else args
        query = str(cell)

    return (query, args)
