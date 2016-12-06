""" Functions and extensions to transparently convert to and from shapely.

    This module provides the "adapt_shapely" and "cast_hexwkb" functions, that
    convert between shapely BaseGeometries and PostGIS's hex-wkb format.
    If the register-functions are called, casting happens transparently behind
    the scenes, and shapely objects can be passed as substitution arguments
    to cursor.execute.

    :author: Gil Georges <gil.georges@lav.mavt.ethz.ch>
    :date: November 23, 2016
    """

import psycopg2.extensions
import shapely.wkb
from shapely.geometry.base import BaseGeometry
import re


class PostGISnotInstalled(Exception):
    """PostGIS not available in current connection."""

    pass


def has_postgis(conn):
    """Check if the connection has access to PostGIS."""
    try:
        get_postgis_version(conn)
    except PostGISnotInstalled:
        return False
    else:
        return True


def get_postgis_version(conn):
    """Return PostGIS version as tuple (major, minor).

    Raises as PostGISnotInstalled Exception if PostGIS is not available.
    Otherwise returns a tuple of integers.

    Returns:
        tuple - int
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT * from PostGIS_Version()")
    except psycopg2.ProgrammingError:
        return False
    else:
        ver = cur.fetchone()[0]
        match = re.match("^([0-9]+)\.([0-9]+)", ver)
        return tuple(int(s) for s in match.groups())
    finally:
        cur.close()
    raise PostGISnotInstalled()


def cast_hexwkb(value, cur):
    """Convert PostGIS 'value' to the corresponding shapely type."""
    if value is None:
        return None
    return shapely.wkb.loads(value, hex=True)


def adapt_shapely(value):
    """Convert a shapely object to PostGIS hex-wkb."""
    wkb = shapely.wkb.dumps(value, hex=True)
    return psycopg2.extensions.AsIs(psycopg2.extensions.adapt(wkb))


def register_postgis2shapely():
    """Register 'cast_hexwkb' to transparently convert results from queries."""
    typ = psycopg2.extensions.new_type((20648,), "GEOGRAPHY", cast_hexwkb)
    psycopg2.extensions.register_type(typ)
    typ = psycopg2.extensions.new_type((20094,), "GEOMETRY", cast_hexwkb)
    psycopg2.extensions.register_type(typ)


def register_shapely2postgis():
    """Register 'adapt_shapely' as an adapter."""
    psycopg2.extensions.register_adapter(BaseGeometry, adapt_shapely)


def activate():
    """Register postgis -> shapely and back."""
    register_postgis2shapely()
    register_shapely2postgis()



