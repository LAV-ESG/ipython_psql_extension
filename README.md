# An IPython extension for Postgres through Psycopg2
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
