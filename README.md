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
0. Make sure you have: *IPython* and *psycopg2* installed. The *PostGIS* integration requires *Shapely*. For this installation instructions to work, you need *pip* installed (use ``python -m pip`` instead of ``pip`` if it's not on the system path).
1. Download the ``.whl`` file of the [latest release](https://github.com/LAV-ESG/ipython_psql_extension/releases/latest)
2. In a console, change to where you downloaded the file and
3. Run:``pip install [name of the file]``

## Installing ``psycopg2``
We recommend using [Anaconda](https://www.continuum.io/downloads) over the legacy CPython binaries from [Python.org](https://python.org), because ``Anaconda`` already ships with most packages you will need for serious data-processing. However, at least on Windows, ``Anaconda`` does not ship with ``psycopg2``. We recommend to ``conda install``the binary from ``conda-forge`` (as it has no issues with SSL support on MacOS, and, on Windows, ``psycopg2`` is actually not on the default channel).

To install ``psycopg2`` run:
```bash
conda install -c conda-forge psycopg2
```

If this does not work, or if you are using legacy CPython, a convenient alternative is to download and ``pip install`` the matching [precompiled wheel](http://www.lfd.uci.edu/~gohlke/pythonlibs/#psycopg) from Christoph Gohlke's homepage (be sure to choose the ``.whl``-File matching your Python interpreter, e.g. "``-cp35-cp35m-win_amd64.whl``" for the Python 3.5, 64-bit version).

## Note: 'sslcert' on MacOSX
On MacOSX, using the 'sslcert' argument in ``%pg_connect`` can cause trouble, as the system seems to expect a separate private key file:
```
ERROR: unable to connect! Got certificate present, but not private key file "/Users/<username>/.postgresql/postgresql.key"
FATAL:  no pg_hba.conf entry for host "<hostname>", user "<username>", database "<database>", SSL off
```

The simplest work-around is to install the certificate locally (just double-click the ``.crt``-File) and set the ``sslcert`` attribute to the empty string:
```Python
%pg_connect sslcert=""
```
