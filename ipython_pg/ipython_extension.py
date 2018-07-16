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
import re
import getpass
from IPython.core.magic import (Magics, line_magic, line_cell_magic,
                                cell_magic, magics_class)
from IPython.display import HTML
import psycopg2
import psycopg2.extensions
import psycopg2.sql
import pandas as pd
import io
import argparse

SQL_SCHEMAS = ("select n.nspname as name, "
               "coalesce(pg_catalog.obj_description(n.oid), "
               "'(no description)') as schema_description, "
               "s.schema_owner as owner "
               "from pg_catalog.pg_namespace n "
               "left join information_schema.schemata s "
               "on s.schema_name = n.nspname "
               "where n.nspname !~ 'pg_(temp|toast|catalog).*' "
               "and n.nspname != 'information_schema'")

SQL_TABLES = ("SELECT c.relname as table_name, "
              "coalesce(pg_catalog.obj_description(c.oid), '(no description)')"
              " as table_description, pg_get_userbyid(c.relowner) AS owner "
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
        self._geo_types = []
        self._preped_stmts = []

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
                    self._geo_types = list(postgis_integration.geo_types())
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
        rxp = re.compile(r"(?<!\$)\${([^}:]*)(?::([si]))?}")
        txt = str(sql)
        q_args = []
        f_args = []

        for expr, fmt in rxp.findall(txt):
            ev = self.shell.ev(expr)
            if fmt:  # if fmt specified, use f-string
                rep = "{}"
                if fmt == "s":
                    ev = psycopg2.sql.Literal(ev)
                elif fmt == "i":
                    if hasattr(ev, 'split'):  # split qualified names
                        ev = ev.split(".")
                    ev = (psycopg2.sql.Identifier(e) for e in ev)
                    ev = psycopg2.sql.SQL(".").join(ev)
                f_args.append(ev)
            else:  # no fmt, treat as query argument
                rep = "%s"
                q_args.append(ev)
            expr = ":".join([expr, fmt]) if fmt else expr
            txt = txt.replace("${%s}" % expr, rep)

        txt = psycopg2.sql.SQL(txt).format(*f_args)
        return txt, q_args

    def query(self, sql, silent=False, propagate=False):
        """Query the database and perform variable substitution.

        Arguments:
            sql {str or SQL} -- SQL command to execute.
            silent {bool} -- if True, only print error messages.
            propagate {bool} -- if True, reraise database errors.
        """
        args = []
        sql = (sql.as_string(self.dbconn) if hasattr(sql, 'as_string')
               else str(sql))
        if "${" in sql:
            sql, args = self._python_tpl(sql)

        try:
            cur = self.pg_cursor()
            cur.execute(sql, args)
            if not silent:
                self.cur_report(cur)
        except psycopg2.Error as e:
            self.shell.write_err("ERROR: {}\n".format(str(e)))
            self.dbconn.rollback()
            if propagate:
                raise e
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
        the results as pandas dataframe. If the PostGIS extension is enabled
        and GeoPandas is installed, the function automatically detects
        GEOMETRY or GEOGRAPHY types and returns a geo-pandas GeoDataFrame
        instead, with is geometry set to the first geo-spatial column.

        Usage:
            %%pg_pd [output] [--idx [IDX] [IDX] ...]
            [query]

        Arguments:
            [output] - name of variable where DataFrame will be stored
            [IDX] - name of index column (warning: does not work if there are
                    white spaces in the name). List multiple names in a white
                    space separated list to create a multi-index. If '--idx' is
                    used without specifying [IDX], the first column will be
                    used by default.
            [query] - SQL query to execute.
        """
        query, args = _line_cell_prep(line, cell)
        args = args if args else ""
        parser = argparse.ArgumentParser()
        parser.add_argument('output', type=str, nargs='?')
        parser.add_argument('--idx', type=str, nargs="+")
        parser.add_argument('--gpd')
        try:
            ns = parser.parse_args(args.strip().split(" "))
        except SystemExit:
            return

        cur = self.query(query)
        dta = self._as_pandas_dataframe(cur, index=ns.idx)

        if ns.output:
            self.shell.write(" results stored as '{}'\n".format(ns.output))
            self.shell.push({ns.output: dta})
            return

        return dta

    def _as_pandas_dataframe(self, cur, index=None):
        if not cur:
            return pd.DataFrame([])
        dta = pd.DataFrame([r for r in cur],
                           columns=[c.name for c in cur.description])
        geocols = [c.name for c in cur.description
                   if c.type_code in self._geo_types]

        if index:
            dta.set_index(index, inplace=True)

        if not geocols:
            return dta

        try:
            import geopandas as gpd
        except ImportError:
            self.shell.write(" warning: GeoPandas not installed; returning "
                             " GIS objects in ordinary pandas DataFrame.")
            return dta

        dta = gpd.GeoDataFrame(dta)
        dta.set_geometry(geocols[0], inplace=True)
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

    @line_magic
    def pg_copy(self, line):
        """Quickly copy data to postgres using native COPY.

        Postgres' `COPY` is intended to move large chunks from and to a
        database. The `on conflict` system of `insert` does not work, i.e. if
        duplicate will beinserted despite primary keys or unique indices).
        So be careful here as this could mess up your table.

        Usage:
            %pg_copy [source] [target]

        Arguments:
            [source] - any Python expression evaluating to a DataFrame
            [target] - name of the target table (DO NOT quotes names!)
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('source', type=str,
                            help="Python expression evaluating to a DataFrame")
        parser.add_argument('target', type=str, help="Target table")
        parser.add_argument('--chunksize', type=int,
                            help=("number of lines to copy at once (their "
                                  "CSV representation needs to fit in memory"))
        try:
            ns = parser.parse_args(line.strip().split(" "))
        except SystemExit:
            return

        dta = self.shell.ev(ns.source)
        if not hasattr(dta, 'to_csv'):
            raise NotImplementedError("currently only works with DataFrames")

        from . import green_mode
        _reactivate = self.green_mode
        if _reactivate:
            self.shell.write("  waring: green-mode temporarily deactivated ("
                             "interrupt won't abort the import)")
            green_mode.deactivate()
        try:
            with self.pg_cursor() as cur:
                copy_pandas_dataframe(cur, dta, ns.target)
            self.dbconn.commit()
        except Exception as e:
            self.dbconn.rollback()
            raise e
        finally:
            if _reactivate:
                green_mode.activate()
                self.shell.write("  green mode reactivated")

    @cell_magic
    def pg_prepare(self, line, cell=None):
        """Execute query as prepared statement.

        Executes the query in the cell as a prepared statement and makes it
        available as a function of the same name. The function must be called
        with as many arguments as were specified in the query (using $<x>
        notation).

        Usage:
            %%pg_prepare <name> [--idx [IDX] ...]

        Arguments:
            <name> -- name of the prepared statement (and callback)
            [IDX] -- field to use as index; if specified, the as_dataframe
                     option is automatically implied.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('name', type=str, nargs='?')
        parser.add_argument('--idx', type=str, nargs="+")

        try:
            ns = parser.parse_args(line.strip().split(" "))
        except SystemExit:
            return

        name = psycopg2.sql.Identifier(ns.name)

        # send deallocate first if redefined
        if ns.name in self._preped_stmts:
            self._preped_stmts.remove(ns.name)
            cur = self.query(psycopg2.sql.SQL("deallocate {}").format(name),
                             silent=True, propagate=True)

        sql = psycopg2.sql.SQL(cell)
        sql = psycopg2.sql.SQL("prepare {} as {}").format(name, sql)
        cur = self.query(sql, silent=True, propagate=True)
        cur.close()

        # only append if the query was successful
        if ns.name not in self._preped_stmts:
            self._preped_stmts.append(ns.name)

        # find number of arguments (assuming $x notation)
        n_args = max([int(s) for s in re.findall("\$([1-9][0-9]*)", cell)])

        # compose execute statement, including placeholders
        sql = [psycopg2.sql.Placeholder()] * n_args
        sql = psycopg2.sql.SQL(", ").join(sql)
        sql = psycopg2.sql.SQL("execute {} ({})").format(name, sql)
        sql = sql.as_string(self.dbconn)

        err_msg = "Prepared statement callback '{}' ".format(ns.name)
        err_msg += "expects {} arguments, ".format(n_args)
        err_msg += "but got {}."

        def callback(*args, df=False, as_dataframe=False):
            if len(args) != n_args:
                raise ValueError(err_msg.format(len(args)))
            try:
                cur = self.pg_cursor()
                cur.execute(sql, args)
            except psycopg2.Error as e:
                self.dbconn.rollback()
                raise e

            if df or as_dataframe:
                return self._as_pandas_dataframe(cur, index=ns.idx)
            return cur

        self.shell.write(" prepared-statement at '{}'\n".format(ns.name))
        self.shell.push({ns.name: callback})

def copy_pandas_dataframe(cur, dta, target, chunk=10000):
    # determine whether we need an index
    index = True
    columns = []
    if dta.index.ndim == 1 and dta.index.names[0] is None:
        # got no index -> skip index
        index = False
    else:
        columns = list(dta.index.names)
        index = True

    # generate copy to command
    target.replace('"', '')
    target = psycopg2.sql.SQL(".").join(psycopg2.sql.Identifier(t.strip())
                                        for t in target.split("."))
    columns = list(dta.columns) + columns
    columns = psycopg2.sql.SQL(", ").join(psycopg2.sql.Identifier(c)
                                          for c in columns)
    sql = psycopg2.sql.SQL('COPY {} ({}) from stdin with (format csv);')
    sql = sql.format(target, columns)

    # write chunk by chunk (to manage memory storage)
    n = len(dta)
    for i in range(0, n, chunk):
        j = min(i + chunk, n)
        buff = io.StringIO()
        dta.iloc[i:j].to_csv(buff, header=False, index=index)
        buff.seek(0)
        cur.copy_expert(sql, buff)
        buff.close()

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
