# An IPython extension for Postgres through Psycopg2
**Download [latest release](https://github.com/LAV-ESG/ipython_psql_extension/releases/latest)**

This IPython extension is a small wrapper around [psycopg2](http://initd.org/psycopg/), to make data-anaylsis between SQL and the Jupyter Notebook more fuild.
The module was written at the [Energy Systems Group of ETHZ-LAV](http://www.lav.ethz.ch/research/energy-systems-group.html), where we use Postgres as our central data repository.

Next to IPython "magics" targeting ease of use, the package also includes:

* variable substitution: query results can be copied to variables in the local scope, and workspace variables enclosed by "{}" are automatically substituted within queries
* "green-mode": a bit of code based on [this article](http://initd.org/psycopg/articles/2014/07/20/cancelling-postgresql-statements-python/) to enable users to interrupt long-running queries
* PostGIS integration: a small bit of wrapper code, transparently converting PostGIS geo-spatial types to Shaply BaseGeometries and back

## Demo
We showcase the extension's features in the [demo notebook](https://github.com/LAV-ESG/ipython_psql_extension/blob/master/IPYpsqglDemo.ipynb).

## Installation

If you use [Anaconda](https://www.continuum.io/downloads) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html), you can install our module straight from our channel:
```bash
conda install -c lav-esg ipython_pg
```

### Note: installing *Shapely*
**Note**: *Shapely* is an optional dependency, meaning it is not installed when you install *ipython_pg*. `ipython_pg` can transparently cast *PostGIS* datatypes to *Shapely* and back.
However this only works if (a) you are connected to a database with *PostGIS* enabled and (b) you have *Shapely* installed.
`ipython_pg` still works without *Shapely*, but all GIS features will be disabled.

To install *Shapely*:
```bash
conda install shapely
```

On many platforms, this will fail. Then use `conda-forge` channel: 
```bash
conda install -c conda-forge shapely
```

### Installation using `pip` wheels
If you are not using `Anaconda` or `Miniconda`:
0. Make sure you have: *IPython*, *Pandas*, *psycopg2* and optionally *Shapely* installed (see note above). 
1. Download the ``.whl`` file of the [latest release](https://github.com/LAV-ESG/ipython_psql_extension/releases/latest)
2. In a console, change to where you downloaded the file
3. Run:``pip install [name of the file]``

If you run into trouble installing `psycopg2`, on Windows you can download and ``pip install`` the matching [precompiled wheel](http://www.lfd.uci.edu/~gohlke/pythonlibs/#psycopg) from Christoph Gohlke's homepage (be sure to choose the ``.whl``-File matching your Python interpreter and platform, e.g. "``-cp35-win_amd64.whl``" for Python 3.5 on 64-bit Windows).

## Troubleshooting: known issues & workarounds

### Unable to get SSL context
While we never figured out why, some version of `psycopg2` seemed to have compatibility issues with other binaries of the Python standard library.
This would result in cryptic error messages, suggestint SSL issues.
In all instances, the solution was reinstalling Anaconda.

### "Got certificate present, but not private key file" on MacOS
On MacOS, the 'sslcert' argument in ``%pg_connect`` is apparently not always set correctly. It seems as if the system would expect a separate private key file:
```
ERROR: unable to connect! Got certificate present, but not private key file "/Users/<username>/.postgresql/postgresql.key"
FATAL:  no pg_hba.conf entry for host "<hostname>", user "<username>", database "<database>", SSL off
```

The simplest work-around is to install the certificate locally (just double-click the ``.crt``-File) and set the ``sslcert`` attribute to the empty string:
```Python
%pg_connect sslcert=""
```

## Tips & tricks

### pgpass
On all platforms, `libpq`, the backend library to the PostgreSQL client, supports a [password file](https://www.postgresql.org/docs/current/libpq-pgpass.html).
Essentially, you type your credentials in a special file, so you do not have to retype your password everytime you connect.
Only do this if your machine is physically secure (i.e. do not do this on mobile devices) and properly shield the file from other users (see [Documentation](https://www.postgresql.org/docs/current/libpq-pgpass.html))
